"""Create or update the Debezium connector through the Kafka Connect REST API.

    python -m src.register_connector

Idempotent: PUT /connectors/{name}/config creates the connector or updates its settings.
Waits until the connector and its task report RUNNING.
"""

from __future__ import annotations

import os
import sys
import time

NAME = "ehr-cdc"
TABLES = ("patients", "encounters", "lab_results", "consents")
PHI_TOPIC_RETENTION_MS = 3 * 24 * 60 * 60 * 1000  # raw PHI is kept in Kafka for 3 days only


def connector_config(env: dict) -> dict:
    json_converter = "org.apache.kafka.connect.json.JsonConverter"
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": env.get("EHR_DB_HOST", "ehr-db"),
        "database.port": env.get("EHR_DB_PORT", "5432"),
        "database.user": "cdc_reader",
        "database.password": env["CDC_PASSWORD"],
        "database.dbname": "ehr",
        "topic.prefix": "ehr",
        "table.include.list": ",".join(f"public.{t}" for t in TABLES),
        # Publication and slot are created by the database owner, not by Debezium
        "publication.name": "ehr_cdc",
        "publication.autocreate.mode": "disabled",
        "slot.name": "ehr_cdc",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        # Confirms progress regularly so the replication slot never holds old WAL
        "heartbeat.interval.ms": "10000",
        "key.converter": json_converter,
        "key.converter.schemas.enable": "false",
        "value.converter": json_converter,
        "value.converter.schemas.enable": "false",
        # Topics Debezium creates for raw PHI: short retention
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "3",
        "topic.creation.default.retention.ms": str(PHI_TOPIC_RETENTION_MS),
        "topic.creation.default.cleanup.policy": "delete",
    }


class ConnectorFailed(RuntimeError):
    """A definite failure: stop waiting and report it."""


def wait_until(check, timeout: float, interval: float = 3, sleep=time.sleep):
    """Poll `check` until it returns something truthy. Transient errors (Connect still starting)
    are retried; ConnectorFailed is raised at once."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except ConnectorFailed:
            raise
        except Exception as err:
            last = err
        sleep(interval)
    raise TimeoutError(f"gave up waiting: {last}")


def register(session, base_url: str, env: dict, timeout: float = 300, sleep=time.sleep) -> dict:
    wait_until(
        lambda: session.get(f"{base_url}/", timeout=5).status_code == 200, timeout, sleep=sleep
    )
    response = session.put(
        f"{base_url}/connectors/{NAME}/config", json=connector_config(env), timeout=30
    )
    if response.status_code >= 400:
        raise ConnectorFailed(
            f"Connect rejected the config: {response.status_code} {response.text}"
        )

    def running():
        status = session.get(f"{base_url}/connectors/{NAME}/status", timeout=5).json()
        tasks = status.get("tasks") or []
        failed = [t for t in tasks if t.get("state") == "FAILED"]
        if failed:
            raise ConnectorFailed(f"connector task failed: {failed[0].get('trace', '')[:2000]}")
        ok = (
            status.get("connector", {}).get("state") == "RUNNING"
            and tasks
            and all(t.get("state") == "RUNNING" for t in tasks)
        )
        return status if ok else False

    return wait_until(running, timeout, sleep=sleep)


def main() -> None:
    import requests

    status = register(
        requests.Session(), os.environ.get("CONNECT_URL", "http://connect:8083"), os.environ
    )
    tasks = [t["state"] for t in status["tasks"]]
    print(f"connector {NAME}: {status['connector']['state']}, tasks: {tasks}")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"registration failed: {err}", file=sys.stderr)
        sys.exit(1)

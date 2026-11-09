import pytest

from src.register_connector import NAME, ConnectorFailed, connector_config, register

ENV = {"CDC_PASSWORD": "cdc-secret"}


class Response:
    def __init__(self, status_code=200, body=None, text=""):
        self.status_code, self._body, self.text = status_code, body or {}, text

    def json(self):
        return self._body


class Connect:
    """Fake Kafka Connect REST API: down at first, then a connector that starts up."""

    def __init__(self, statuses, down_for=1, put_status=201):
        self.statuses, self.down_for, self.put_status = list(statuses), down_for, put_status
        self.put_body = None

    def get(self, url, timeout):
        if url.endswith("/"):
            if self.down_for:
                self.down_for -= 1
                raise ConnectionError("connect not started")
            return Response(200)
        return Response(200, self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0])

    def put(self, url, json, timeout):
        assert url.endswith(f"/connectors/{NAME}/config")
        self.put_body = json
        return Response(self.put_status, text="bad config")


RUNNING = {"connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "RUNNING"}]}


def test_config_is_least_privilege_and_short_retention():
    config = connector_config(ENV)
    assert config["database.user"] == "cdc_reader"
    assert (
        config["publication.autocreate.mode"] == "disabled"
    )  # Debezium cannot widen what it reads
    tables = ["patients", "encounters", "lab_results", "consents"]
    assert config["table.include.list"].split(",") == [f"public.{t}" for t in tables]
    assert int(config["topic.creation.default.retention.ms"]) == 3 * 24 * 3600 * 1000
    assert config["heartbeat.interval.ms"] == "10000"


def test_register_waits_for_connect_then_for_running():
    connect = Connect([{"connector": {"state": "UNASSIGNED"}, "tasks": []}, RUNNING])
    status = register(connect, "http://connect:8083", ENV, timeout=5, sleep=lambda s: None)
    assert status == RUNNING
    assert connect.put_body["database.password"] == "cdc-secret"


def test_failed_task_is_reported_immediately():
    task = {"id": 0, "state": "FAILED", "trace": "replication slot is in use"}
    failed = {"connector": {"state": "RUNNING"}, "tasks": [task]}
    with pytest.raises(ConnectorFailed, match="replication slot is in use"):
        register(Connect([failed], down_for=0), "http://c", ENV, timeout=60,
                 sleep=lambda s: None)  # fmt: skip


def test_rejected_config_is_reported():
    with pytest.raises(ConnectorFailed, match="rejected the config: 400"):
        register(Connect([RUNNING], down_for=0, put_status=400), "http://c", ENV, timeout=5,
                 sleep=lambda s: None)  # fmt: skip

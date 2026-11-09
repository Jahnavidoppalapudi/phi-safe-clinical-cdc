"""Build events as the Debezium PostgreSQL connector writes them with this project's settings:
JsonConverter without schemas, time.precision.mode=connect, decimal.handling.mode=string."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal

EPOCH = date(1970, 1, 1)
PRIMARY_KEYS = {"patients": "patient_id", "encounters": "encounter_id",
                "lab_results": "lab_id", "consents": "patient_id"}  # fmt: skip


def encode_row(row: dict | None) -> dict | None:
    if row is None:
        return None
    out = {}
    for column, value in row.items():
        if isinstance(value, datetime):
            out[column] = value.astimezone(UTC).isoformat().replace("+00:00", "Z")
        elif isinstance(value, date):
            out[column] = (value - EPOCH).days
        elif isinstance(value, Decimal):
            out[column] = str(value)
        else:
            out[column] = value
    return out


def debezium_message(table: str, op: str, row: dict, lsn: int, before: dict | None = None):
    """(topic, key bytes, value bytes) for one change."""
    key = {PRIMARY_KEYS[table]: row[PRIMARY_KEYS[table]]}
    value = {
        "before": encode_row(row if op == "d" else before),
        "after": None if op == "d" else encode_row(row),
        "op": op,
        "source": {"connector": "postgresql", "name": "ehr", "db": "ehr", "schema": "public",
                   "table": table, "lsn": lsn, "txId": lsn // 10, "ts_ms": 1794241138000,
                   "snapshot": "true" if op == "r" else "false"},
        "ts_ms": 1794241139000,
    }  # fmt: skip
    return f"ehr.public.{table}", json.dumps(key).encode(), json.dumps(value).encode()

"""Parse Debezium change events from the EHR database.

Debezium (PostgreSQL connector, JSON without embedded schemas) sends for every row change:
    {"before": {...} | null, "after": {...} | null, "op": "c|u|d|r",
     "source": {"table": ..., "lsn": ..., "txId": ..., "ts_ms": ..., "snapshot": ...}, "ts_ms": ...}

With the connector settings used here (time.precision.mode=connect, decimal.handling.mode=string):
dates arrive as days since 1970-01-01, timestamptz as ISO-8601 strings, numerics as strings.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

OPS = {"c": "insert", "r": "snapshot", "u": "update", "d": "delete"}
EPOCH = date(1970, 1, 1)

# Column types of the source tables: the contract this parser relies on
DATE_COLUMNS = {"date_of_birth"}
DECIMAL_COLUMNS = {"value_numeric", "reference_low", "reference_high"}


class MalformedEvent(ValueError):
    pass


def to_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, int):
        return EPOCH + timedelta(days=value)
    return date.fromisoformat(value)


def to_timestamp(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, int):  # epoch milliseconds
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise MalformedEvent(f"timestamp without time zone: {value!r}")
    return parsed.astimezone(UTC)


def coerce_row(row: dict | None) -> dict | None:
    if row is None:
        return None
    out = {}
    for column, value in row.items():
        if column in DATE_COLUMNS:
            out[column] = to_date(value)
        elif column.endswith("_at"):
            out[column] = to_timestamp(value)
        elif column in DECIMAL_COLUMNS and value is not None:
            out[column] = Decimal(value)
        else:
            out[column] = value
    return out


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    op: str  # insert, snapshot, update, delete
    before: dict | None
    after: dict | None
    lsn: int | None  # position in the PostgreSQL write-ahead log
    tx_id: int | None
    committed_at: datetime  # when the change was committed in the source database

    @property
    def row(self) -> dict:
        """The row as it is now (after), or as it was when deleted (before)."""
        return self.after if self.after is not None else self.before

    @property
    def patient_id(self):
        return self.row.get("patient_id")


def parse_event(value: bytes | str | dict | None) -> ChangeEvent | None:
    """None for tombstones and heartbeats, which carry no row change."""
    if value is None:
        return None
    event = json.loads(value) if isinstance(value, bytes | str) else value
    if "payload" in event and "schema" in event:  # tolerate schemas.enable=true
        event = event["payload"]
    op = event.get("op")
    if op is None:
        return None  # heartbeat or other control message
    if op not in OPS:
        raise MalformedEvent(f"unsupported operation {op!r}")
    source = event.get("source") or {}
    if not source.get("table"):
        raise MalformedEvent("event has no source table")
    before, after = coerce_row(event.get("before")), coerce_row(event.get("after"))
    if (op == "d" and before is None) or (op != "d" and after is None):
        raise MalformedEvent(f"{OPS[op]} event without the row image it needs")
    return ChangeEvent(
        table=source["table"],
        op=OPS[op],
        before=before,
        after=after,
        lsn=source.get("lsn"),
        tx_id=source.get("txId"),
        committed_at=datetime.fromtimestamp(
            source.get("ts_ms", event.get("ts_ms", 0)) / 1000, tz=UTC
        ),
    )

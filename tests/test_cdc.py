"""Debezium event parsing, using events shaped like the PostgreSQL connector's JSON output."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from src.cdc import MalformedEvent, parse_event

SOURCE = {"version": "3.6.3.Final", "connector": "postgresql", "name": "ehr",
          "ts_ms": 1794241138000, "snapshot": "false", "db": "ehr", "schema": "public",
          "table": "patients", "txId": 812, "lsn": 24023128}  # fmt: skip
PATIENT = {"patient_id": 17, "mrn": "MRN-12345678", "first_name": "Avery",
           "date_of_birth": -3215,  # days since 1970-01-01 -> 1961-03-14
           "zip": "78701", "created_at": "2026-08-01T09:30:00.123456Z"}  # fmt: skip
LAB = {"lab_id": 4, "encounter_id": 2, "patient_id": 17, "value_numeric": "5.120",
       "reference_low": "3.500", "collected_at": "2026-08-01T10:00:00Z"}  # fmt: skip


def event(op, before=None, after=None, table="patients", **source):
    body = {"before": before, "after": after, "op": op,
            "source": {**SOURCE, "table": table, **source}, "ts_ms": 1794241139000}  # fmt: skip
    return json.dumps(body).encode()


def test_insert_is_typed():
    e = parse_event(event("c", after=PATIENT))
    assert (e.table, e.op, e.lsn, e.tx_id) == ("patients", "insert", 24023128, 812)
    assert e.after["date_of_birth"] == date(1961, 3, 14)
    assert e.after["created_at"] == datetime(2026, 8, 1, 9, 30, 0, 123456, tzinfo=UTC)
    assert e.committed_at == datetime.fromtimestamp(1794241138, tz=UTC)
    assert e.patient_id == 17


def test_numeric_strings_become_exact_decimals():
    e = parse_event(event("c", after=LAB, table="lab_results"))
    assert e.after["value_numeric"] == Decimal("5.120") and e.after["reference_low"] == Decimal(
        "3.5"
    )


def test_snapshot_update_and_delete():
    assert parse_event(event("r", after=PATIENT, snapshot="true")).op == "snapshot"
    moved = {**PATIENT, "zip": "05901"}
    update = parse_event(event("u", before=PATIENT, after=moved))
    assert (update.before["zip"], update.after["zip"]) == ("78701", "05901")
    delete = parse_event(event("d", before=PATIENT))
    assert delete.op == "delete" and delete.after is None
    assert delete.row["patient_id"] == 17 and delete.patient_id == 17  # identity from 'before'


def test_tombstones_and_heartbeats_carry_no_change():
    assert parse_event(None) is None
    assert parse_event(json.dumps({"ts_ms": 1794241139000}).encode()) is None


def test_envelope_with_embedded_schema_is_accepted():
    wrapped = {"schema": {"type": "struct"}, "payload": json.loads(event("c", after=PATIENT))}
    assert parse_event(json.dumps(wrapped)).after["patient_id"] == 17


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (event("t"), "unsupported operation"),
        (event("c", after=None), "without the row image"),
        (event("d", before=None), "without the row image"),
        (event("c", after=PATIENT, table=None), "no source table"),
        (event("c", after={**PATIENT, "created_at": "2026-08-01T09:30:00"}), "without time zone"),
    ],
)
def test_malformed_events_are_rejected(raw, message):
    with pytest.raises(MalformedEvent, match=message):
        parse_event(raw)

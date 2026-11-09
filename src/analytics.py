"""Apply de-identified change events to the analytics zone.

Rules, all enforced here and tested:
- Order: a row is only overwritten by an event with a later source LSN (position in the EHR's
  write-ahead log), so delayed or replayed events can never roll data back.
- Deletes are remembered (deleted_rows), so a replayed older insert cannot resurrect a row.
- Consent: once a patient withdraws research consent, all their rows are purged and new events
  for them are not stored. Patients whose consent is unknown or not given are stored de-identified
  but hidden from analysts (the v_* views only show consenting patients).
- Every purge, deletion and unclassified column is recorded in audit_log, without PHI.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from src.cdc import ChangeEvent
from src.deid import Deidentifier

SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "analytics" / "schema.sql"

# source table -> (analytics table, token column that identifies the row)
TARGETS = {
    "patients": ("patients_deid", "patient_token"),
    "encounters": ("encounters_deid", "encounter_token"),
    "lab_results": ("labs_deid", "lab_token"),
}
PATIENT_TABLES = ("labs_deid", "encounters_deid", "patients_deid")


def apply_schema(conn, analyst_password: str | None = None, analyst_role: str = "analyst") -> None:
    """Tables, views, and a login that can read the consent-gated views and nothing else."""
    from psycopg import sql

    with conn.transaction():
        conn.execute("select pg_advisory_xact_lock(4243)")
        conn.execute(SCHEMA.read_text())
        if analyst_password:
            role = sql.Identifier(analyst_role)
            exists = conn.execute(
                "select 1 from pg_roles where rolname = %s", (analyst_role,)
            ).fetchone()
            verb = sql.SQL("alter" if exists else "create")
            conn.execute(sql.SQL("{} role {} login password {}").format(
                verb, role, sql.Literal(analyst_password)))  # fmt: skip
            schema = conn.execute("select current_schema()").fetchone()[0]
            conn.execute(
                sql.SQL("grant usage on schema {} to {}").format(sql.Identifier(schema), role)
            )
            views = conn.execute(
                "select viewname from pg_views where schemaname = %s and viewname like 'v\\_%%'",
                (schema,),
            ).fetchall()
            for (view,) in views:
                target = sql.Identifier(schema, view)
                conn.execute(sql.SQL("grant select on {} to {}").format(target, role))


@dataclass
class ApplyResult:
    applied: int = 0
    stale_skipped: int = 0
    held_no_consent: int = 0
    audit: list[tuple] = field(default_factory=list)


def audit(cur, action: str, patient_token=None, table_name=None, **details) -> None:
    cur.execute(
        "insert into audit_log (action, patient_token, table_name, details) "
        "values (%s, %s, %s, %s)",
        (action, patient_token, table_name, json.dumps(details, default=str)),
    )


def consent_of(cur, patient_token: str) -> bool | None:
    row = cur.execute(
        "select research_use from consent_state where patient_token = %s", (patient_token,)
    ).fetchone()
    return row[0] if row else None


def purge_patient(cur, patient_token: str) -> dict[str, int]:
    removed = {}
    for table in PATIENT_TABLES:
        removed[table] = cur.execute(
            f"delete from {table} where patient_token = %s", (patient_token,)
        ).rowcount
    return removed


def apply_consent(cur, event: ChangeEvent, deid: Deidentifier, result: ApplyResult) -> None:
    token = deid.token("patient", event.patient_id)
    research_use = False if event.op == "delete" else bool(event.row["research_use"])
    previous = cur.execute(
        "select research_use, source_lsn from consent_state where patient_token = %s", (token,)
    ).fetchone()
    if previous and previous[1] > event.lsn:
        result.stale_skipped += 1
        return
    cur.execute(
        """insert into consent_state values (%s, %s, %s)
           on conflict (patient_token) do update
           set research_use = excluded.research_use, source_lsn = excluded.source_lsn""",
        (token, research_use, event.lsn),
    )
    result.applied += 1
    was = previous[0] if previous else None
    if was is not False and not research_use:
        removed = purge_patient(cur, token)
        audit(cur, "consent_withdrawn_purge", token, "consents", rows_removed=removed)
    elif was is False and research_use:
        audit(cur, "consent_granted", token, "consents",
              note="earlier data was purged; a re-snapshot is needed to restore it")  # fmt: skip


def apply_row_event(cur, event: ChangeEvent, deid: Deidentifier, result: ApplyResult) -> None:
    table, key_column = TARGETS[event.table]
    row, unclassified = deid.apply(event.table, event.row)
    if unclassified:
        audit(cur, "unclassified_columns_dropped", None, event.table, columns=unclassified)
    token = row[key_column]
    patient_token = row["patient_token"]

    tombstone = cur.execute(
        "select deleted_lsn from deleted_rows where table_name = %s and row_token = %s",
        (table, token),
    ).fetchone()
    if tombstone and tombstone[0] >= event.lsn:
        result.stale_skipped += 1  # older than a deletion we already applied
        return

    if event.op == "delete":
        cur.execute(f"delete from {table} where {key_column} = %s and source_lsn <= %s",
                    (token, event.lsn))  # fmt: skip
        cur.execute(
            """insert into deleted_rows values (%s, %s, %s)
               on conflict (table_name, row_token)
               do update set deleted_lsn = excluded.deleted_lsn""",
            (table, token, event.lsn),
        )
        if table == "patients_deid":
            audit(cur, "source_record_deleted", patient_token, event.table)
        result.applied += 1
        return

    if consent_of(cur, patient_token) is False:
        result.held_no_consent += 1  # withdrawn: never store new data for this patient
        return

    columns = list(row) + ["source_lsn"]
    values = list(row.values()) + [event.lsn]
    updates = ", ".join(f"{c} = excluded.{c}" for c in columns if c != key_column)
    written = cur.execute(
        f"""insert into {table} as t ({", ".join(columns)})
            values ({", ".join(["%s"] * len(values))})
            on conflict ({key_column}) do update set {updates}
            where t.source_lsn <= excluded.source_lsn""",
        values,
    ).rowcount
    if written:
        result.applied += 1
    else:
        result.stale_skipped += 1


def apply_event(cur, event: ChangeEvent, deid: Deidentifier, result: ApplyResult) -> None:
    if event.lsn is None:
        raise ValueError("change event without an LSN cannot be ordered")
    if event.table == "consents":
        apply_consent(cur, event, deid, result)
    elif event.table in TARGETS:
        apply_row_event(cur, event, deid, result)
    else:
        audit(cur, "unknown_table_ignored", None, event.table)

"""The EHR source database, built from the same SQL the container runs."""

import random
import re
from datetime import UTC, datetime, timedelta

import pytest
from conftest import run_role_script

from src.simulator import Simulator

SOURCE_TABLES = ("patients", "encounters", "lab_results", "consents")


def test_every_source_column_has_a_privacy_decision(ehr_db, policy):
    """Contract between the database and the policy: no column may be unclassified."""
    columns = ehr_db.execute(
        "select table_name, column_name from information_schema.columns "
        "where table_schema = 'public' and table_name = any(%s)",
        (list(SOURCE_TABLES),),
    ).fetchall()
    missing = [f"{t}.{c}" for t, c in columns if c not in policy.get(t, {})]
    stale = [f"{t}.{c}" for t in policy for c in policy[t] if (t, c) not in set(columns)]
    assert missing == [], f"columns without a de-identification rule: {missing}"
    assert stale == [], f"policy rules for columns that no longer exist: {stale}"


def test_publication_and_full_row_images(ehr_db):
    published = ehr_db.execute(
        "select tablename from pg_publication_tables where pubname = 'ehr_cdc' order by 1"
    ).fetchall()
    assert [t for (t,) in published] == sorted(SOURCE_TABLES)
    identity = dict(ehr_db.execute(
        "select relname, relreplident from pg_class where relname = any(%s)", (list(SOURCE_TABLES),)
    ).fetchall())  # fmt: skip
    assert set(identity.values()) == {"f"}  # FULL: updates and deletes carry the whole old row


def test_cdc_login_can_read_but_not_change(ehr_db):
    psycopg = pytest.importorskip("psycopg")
    from psycopg.conninfo import make_conninfo

    params = ehr_db.info_params
    exists = ehr_db.execute("select 1 from pg_roles where rolname = 'cdc_reader'").fetchone()
    if exists:
        pytest.skip("cdc_reader already exists in this cluster (shared CI database)")
    result = run_role_script(params, "cdc-test-password")
    assert result.returncode == 0, result.stderr
    try:
        login = {**params, "user": "cdc_reader", "password": "cdc-test-password"}
        reader = psycopg.connect(make_conninfo(**login), autocommit=True)
        with reader:
            assert reader.execute("select count(*) from patients").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                reader.execute("delete from patients")
            attrs = reader.execute(
                "select rolreplication, rolsuper from pg_roles where rolname = current_user"
            ).fetchone()
            assert attrs == (True, False)
    finally:
        ehr_db.execute("drop owned by cdc_reader")
        ehr_db.execute("drop role cdc_reader")


def test_changes_stream_with_full_before_images_and_cascades(ehr_db):
    if ehr_db.execute("show wal_level").fetchone()[0] != "logical":
        pytest.skip("wal_level is not logical")
    sim = Simulator(ehr_db, rng=random.Random(1))
    at = datetime.now(UTC) - timedelta(days=2)
    pid = sim.new_patient(at)
    sim.admit(at)  # creates an encounter and labs for some patient (the only one: pid)

    ehr_db.execute("select pg_create_logical_replication_slot('test_slot', 'test_decoding')")
    ehr_db.execute("update patients set zip = '05901' where patient_id = %s", (pid,))
    ehr_db.execute("delete from patients where patient_id = %s", (pid,))
    changes = [r[0] for r in ehr_db.execute(
        "select data from pg_logical_slot_get_changes('test_slot', null, null)"
    ).fetchall()]  # fmt: skip

    update = next(c for c in changes if c.startswith("table public.patients: UPDATE"))
    assert "old-key:" in update and "ssn[text]" in update.split("new-tuple:")[0]  # whole old row
    deleted = {m.group(1) for c in changes if (m := re.match(r"table public\.(\w+): DELETE", c))}
    assert deleted == set(SOURCE_TABLES)  # the chart and everything under it


def test_simulator_backfill_is_realistic_and_synthetic(ehr_db):
    sim = Simulator(ehr_db, rng=random.Random(3))
    clock = datetime.now(UTC) - timedelta(days=20)
    for _ in range(20 * 12):
        clock += timedelta(hours=2)
        sim.step(clock, 2)
    q = lambda sql: ehr_db.execute(sql).fetchone()[0]  # noqa: E731
    assert q("select count(*) from patients") > 200 and q("select count(*) from lab_results") > 500
    assert q("select count(*) from patients where ssn !~ '^9[0-9]{2}-'") == 0  # never-issued SSNs
    assert q("select count(*) from patients where phone !~ '555-01[0-9]{2}$'") == 0
    assert q("select count(*) from patients where email not like '%@example.org'") == 0
    assert q("select count(*) from encounters where notes ~ 'Pt [A-Z]'") > 0  # notes do carry names
    los = q("""select avg(extract(epoch from discharged_at - admitted_at) / 86400)
               from encounters where encounter_type = 'inpatient' and status = 'discharged'""")
    assert 3 < los < 6.5  # days, near the US average

import os
import shutil
import subprocess
import uuid
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
TEST_KEY = b"k" * 32


@pytest.fixture
def policy():
    from src.deid import load_policy

    return load_policy(ROOT / "config/deid_policy.yaml")


@pytest.fixture
def deid(policy):
    from src.deid import Deidentifier

    return Deidentifier(TEST_KEY, policy, today=date(2026, 11, 9))


@contextmanager
def fresh_ehr_db():
    """A fresh EHR database built from sql/ehr, the same scripts the container runs."""
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL not set")
    psycopg = pytest.importorskip("psycopg")
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    name = f"ehr_{uuid.uuid4().hex[:10]}"
    admin = psycopg.connect(TEST_DATABASE_URL, autocommit=True)
    admin.execute(f"create database {name}")
    params = {**conninfo_to_dict(TEST_DATABASE_URL), "dbname": name}
    conn = psycopg.connect(make_conninfo(**params), autocommit=True)
    conn.execute((ROOT / "sql/ehr/01_schema.sql").read_text())
    conn.info_params = params
    try:
        yield conn
    finally:
        conn.close()
        for slot in admin.execute(
            "select slot_name from pg_replication_slots where database = %s", (name,)
        ).fetchall():
            admin.execute("select pg_drop_replication_slot(%s)", slot)
        admin.execute(f"drop database {name} with (force)")
        admin.close()


@contextmanager
def fresh_analytics_schema():
    """Analytics-zone tables and views in a fresh, throwaway schema."""
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL not set")
    psycopg = pytest.importorskip("psycopg")
    from src.analytics import apply_schema

    schema = f"analytics_{uuid.uuid4().hex[:10]}"
    conn = psycopg.connect(TEST_DATABASE_URL, autocommit=True)
    conn.execute(f"create schema {schema}")
    conn.execute(f"set search_path to {schema}")
    apply_schema(conn)
    conn.schema = schema
    try:
        yield conn
    finally:
        conn.execute(f"drop schema {schema} cascade")
        conn.close()


@pytest.fixture
def ehr_db():
    with fresh_ehr_db() as conn:
        yield conn


@pytest.fixture
def analytics_db():
    with fresh_analytics_schema() as conn:
        yield conn


def run_role_script(params: dict, password: str) -> subprocess.CompletedProcess:
    """Run sql/ehr/02_cdc_role.sh exactly as the container would."""
    if not shutil.which("psql"):
        pytest.skip("psql client not installed")
    env = {**os.environ, "POSTGRES_USER": params.get("user", "postgres"),
           "POSTGRES_DB": params["dbname"], "CDC_PASSWORD": password,
           "PGHOST": params.get("host", "localhost"), "PGPORT": str(params.get("port", 5432)),
           "PGPASSWORD": params.get("password", "")}  # fmt: skip
    return subprocess.run(["sh", str(ROOT / "sql/ehr/02_cdc_role.sh")], env=env,
                          capture_output=True, text=True)  # fmt: skip


def change(table, op, row, lsn, before=None):
    """A parsed CDC event, as src.cdc.parse_event would return it."""
    from datetime import UTC, datetime

    from src.cdc import ChangeEvent

    ops = {"c": "insert", "r": "snapshot", "u": "update", "d": "delete"}
    return ChangeEvent(table=table, op=ops[op],
                       before=row if op == "d" else before, after=None if op == "d" else row,
                       lsn=lsn, tx_id=None, committed_at=datetime.now(UTC))  # fmt: skip

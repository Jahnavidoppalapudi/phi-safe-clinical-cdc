"""EHR -> Debezium-format events -> Kafka (mock broker) -> processor -> analytics zone.

The hospital simulator writes 30 days of activity. Every row is turned into a Debezium snapshot
event; then a consent withdrawal and a duplicate-chart deletion are streamed as live changes.
"""

import random
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from conftest import TEST_KEY, fresh_analytics_schema, fresh_ehr_db
from debezium_events import debezium_message

pytest.importorskip("confluent_kafka")
from confluent_kafka import Consumer, Producer  # noqa: E402

from src.leak_scan import scan  # noqa: E402
from src.processor import TOPICS, consumer_config, run  # noqa: E402
from src.simulator import Simulator  # noqa: E402

TABLES = ("patients", "encounters", "lab_results", "consents")

# Same definition as v_readmissions_30d, computed on the raw EHR data
EHR_READMISSIONS = """
with eligible as (
    select patient_id from consents where research_use
), stays as (
    select e.* from encounters as e join eligible using (patient_id)
), index_stays as (
    select * from stays where encounter_type = 'inpatient' and status = 'discharged'
)
select count(*), count(*) filter (where exists (
    select 1 from stays as r
    where r.patient_id = s.patient_id and r.encounter_type = 'inpatient'
      and r.status <> 'cancelled'
      and r.admitted_at > s.discharged_at
      and r.admitted_at <= s.discharged_at + interval '30 days'))
from index_stays as s
"""


def rows(db, table):
    from psycopg.rows import dict_row

    with db.cursor(row_factory=dict_row) as cur:
        return cur.execute(f"select * from {table}").fetchall()


def process_all(bootstrap, analytics_db, deid, group, expected):
    consumer = Consumer({**consumer_config(bootstrap), "group.id": group, "log_level": 0})
    consumer.subscribe(TOPICS)
    loops = []

    def stop():
        loops.append(1)
        seen = analytics_db.execute(
            "select coalesce(sum(events), 0) from processor_batches"
        ).fetchone()[0]
        return seen >= expected or len(loops) > 60

    run(consumer, analytics_db, deid, stop)
    consumer.close()


@pytest.fixture(scope="module")
def stack():
    """Built once for the whole module: 30 simulated days take a while to stream."""
    with fresh_ehr_db() as ehr, fresh_analytics_schema() as analytics:
        yield ehr, analytics


@pytest.fixture(scope="module")
def module_deid():
    from pathlib import Path

    from src.deid import Deidentifier, load_policy

    policy = load_policy(Path(__file__).resolve().parents[1] / "config/deid_policy.yaml")
    return Deidentifier(TEST_KEY, policy)


@pytest.fixture
def ehr_db(stack):
    return stack[0]


@pytest.fixture
def analytics_db(stack):
    return stack[1]


@pytest.fixture
def deid(module_deid):
    return module_deid


@pytest.fixture(scope="module")
def pipeline(stack, module_deid):
    ehr_db, analytics_db = stack
    deid = module_deid
    sim = Simulator(ehr_db, rng=random.Random(11))
    clock = datetime.now(UTC) - timedelta(days=30)
    for _ in range(30 * 12):
        clock += timedelta(hours=2)
        sim.step(clock, 2)

    producer = Producer({"test.mock.num.brokers": 1, "log_level": 0})
    bootstrap = producer.list_topics(timeout=10).orig_broker_name.split("/")[0]
    sent = 0

    def send(table, op, row, lsn, before=None):
        nonlocal sent
        topic, key, value = debezium_message(table, op, row, lsn, before)
        producer.produce(topic, key=key, value=value)
        sent += 1

    # 1. Initial snapshot of every table
    for table in TABLES:
        for row in rows(ehr_db, table):
            send(table, "r", row, lsn=1000)

    # 2. Live change: a consenting patient with hospital stays withdraws research consent
    withdrawn = ehr_db.execute(
        """select c.patient_id from consents c join encounters e using (patient_id)
           where c.research_use and e.encounter_type = 'inpatient' limit 1"""
    ).fetchone()[0]
    before = next(r for r in rows(ehr_db, "consents") if r["patient_id"] == withdrawn)
    ehr_db.execute("update consents set research_use = false where patient_id = %s", (withdrawn,))
    after = next(r for r in rows(ehr_db, "consents") if r["patient_id"] == withdrawn)
    send("consents", "u", after, lsn=2000, before=before)

    # 3. Live change: a duplicate chart is deleted; the delete cascades to every child row
    deleted = ehr_db.execute(
        """select c.patient_id from consents c join encounters e using (patient_id)
           where c.research_use and c.patient_id <> %s limit 1""",
        (withdrawn,),
    ).fetchone()[0]
    cascade = [(t, r) for t in ("lab_results", "encounters", "consents", "patients")
               for r in rows(ehr_db, t) if r["patient_id"] == deleted]  # fmt: skip
    ehr_db.execute("delete from patients where patient_id = %s", (deleted,))
    for i, (table, row) in enumerate(cascade):
        send(table, "d", row, lsn=3000 + i)

    producer.flush(30)
    process_all(bootstrap, analytics_db, deid, "deid-e2e", expected=sent)
    return {"bootstrap": bootstrap, "sent": sent, "withdrawn": withdrawn, "deleted": deleted}


def test_no_identifier_reaches_the_analytics_zone(pipeline, analytics_db, ehr_db):
    assert analytics_db.execute("select count(*) from encounters_deid").fetchone()[0] > 300
    assert scan(analytics_db) == []
    # No raw value of any direct identifier appears anywhere, in any form
    identifiers = {v for r in rows(ehr_db, "patients")
                   for c in ("mrn", "first_name", "last_name", "ssn", "phone", "email", "street")
                   if (v := r[c])}  # fmt: skip
    stored = []
    for table in ("patients_deid", "encounters_deid", "labs_deid", "audit_log"):
        stored += [str(v) for row in analytics_db.execute(f"select * from {table}") for v in row]
    dump = " ".join(stored)
    assert [i for i in identifiers if i in dump] == []


def test_readmission_rate_survives_de_identification_exactly(pipeline, analytics_db, ehr_db):
    discharges, readmitted = ehr_db.execute(EHR_READMISSIONS).fetchone()
    analytics = analytics_db.execute(
        "select sum(discharges), sum(readmitted_30d) from v_readmissions_30d"
    ).fetchone()
    assert discharges > 50 and readmitted > 0
    assert analytics == (discharges, readmitted)  # dates shifted, intervals intact


def test_length_of_stay_is_identical(pipeline, analytics_db, ehr_db):
    raw = ehr_db.execute(
        """select round((avg(extract(epoch from discharged_at - admitted_at)) / 86400)::numeric, 2)
           from encounters e join consents c using (patient_id)
           where c.research_use and status = 'discharged' and encounter_type = 'inpatient'"""
    ).fetchone()[0]
    deid = analytics_db.execute(
        """select round((avg(extract(epoch from discharged_at - admitted_at)) / 86400)::numeric, 2)
           from v_encounters where status = 'discharged' and encounter_type = 'inpatient'"""
    ).fetchone()[0]
    assert deid == raw


def test_withdrawn_and_deleted_patients_are_gone_and_audited(pipeline, analytics_db, deid):
    for key in ("withdrawn", "deleted"):
        token = deid.token("patient", pipeline[key])
        for table in ("patients_deid", "encounters_deid", "labs_deid"):
            assert analytics_db.execute(
                f"select count(*) from {table} where patient_token = %s", (token,)
            ).fetchone() == (0,), f"{key} patient still in {table}"
    actions = dict(analytics_db.execute(
        "select action, count(*) from audit_log group by action").fetchall())  # fmt: skip
    assert actions["consent_withdrawn_purge"] >= 1 and actions["source_record_deleted"] == 1


def test_replaying_the_whole_stream_changes_nothing(pipeline, analytics_db, deid):
    def fingerprint():
        return analytics_db.execute(
            """select (select md5(string_agg(t::text, '' order by encounter_token))
                       from encounters_deid t),
                      (select md5(string_agg(t::text, '' order by lab_token)) from labs_deid t),
                      (select count(*) from patients_deid), (select count(*) from consent_state)"""
        ).fetchone()

    before = fingerprint()
    process_all(pipeline["bootstrap"], analytics_db, deid, f"replay-{uuid.uuid4()}",
                expected=2 * pipeline["sent"])  # fmt: skip
    assert fingerprint() == before


def test_wrong_key_produces_unlinkable_tokens(pipeline, policy):
    from src.deid import Deidentifier

    assert Deidentifier(TEST_KEY, policy).token("patient", 1) != Deidentifier(
        bytes(reversed(TEST_KEY + b"x")), policy
    ).token("patient", 1)

"""Analytics-zone rules against a real PostgreSQL: ordering, deletes, consent, audit."""

from datetime import UTC, datetime

import pytest
from conftest import change

from src.analytics import ApplyResult, apply_event

T0 = datetime(2026, 5, 1, 8, tzinfo=UTC)
PATIENT = {"patient_id": 1, "mrn": "MRN-11112222", "first_name": "Avery", "last_name": "Rivera",
           "date_of_birth": datetime(1961, 3, 14).date(), "sex": "F", "ssn": "912-34-5678",
           "phone": "(512) 555-0142", "email": "a@example.org", "street": "1 Oak St",
           "city": "Austin", "state": "TX", "zip": "78701",
           "created_at": T0, "updated_at": T0}  # fmt: skip
ENCOUNTER = {"encounter_id": 10, "patient_id": 1, "encounter_type": "inpatient",
             "department": "Cardiology", "admitted_at": T0, "discharged_at": None,
             "primary_dx_code": "I50.9", "status": "admitted", "attending_npi": "1234567890",
             "notes": "Pt Avery Rivera", "updated_at": T0}  # fmt: skip


def consent(research_use, lsn, patient_id=1):
    return change("consents", "u", {"patient_id": patient_id, "research_use": research_use,
                                    "updated_at": T0}, lsn)  # fmt: skip


def apply(db, deid, *events):
    result = ApplyResult()
    with db.transaction(), db.cursor() as cur:
        for e in events:
            apply_event(cur, e, deid, result)
    return result


def count(db, sql):
    return db.execute(sql).fetchone()[0]


def test_newer_versions_win_and_older_ones_are_skipped(analytics_db, deid):
    discharged = {
        **ENCOUNTER,
        "status": "discharged",
        "discharged_at": datetime(2026, 5, 4, tzinfo=UTC),
    }
    result = apply(analytics_db, deid, consent(True, 1),
                   change("encounters", "c", ENCOUNTER, 100),
                   change("encounters", "u", discharged, 300),
                   change("encounters", "u", ENCOUNTER, 200))  # delayed older event  # fmt: skip
    assert result.stale_skipped == 1
    assert count(analytics_db, "select status from encounters_deid") == "discharged"


def test_deleted_rows_cannot_be_resurrected_by_replays(analytics_db, deid):
    apply(analytics_db, deid, consent(True, 1), change("encounters", "c", ENCOUNTER, 100),
          change("encounters", "d", ENCOUNTER, 200))  # fmt: skip
    replay = apply(analytics_db, deid, change("encounters", "c", ENCOUNTER, 100))
    assert replay.stale_skipped == 1
    assert count(analytics_db, "select count(*) from encounters_deid") == 0


def test_only_consenting_patients_are_visible_to_analysts(analytics_db, deid):
    apply(analytics_db, deid, change("patients", "c", PATIENT, 100),
          change("encounters", "c", ENCOUNTER, 101))  # fmt: skip
    # Consent not known yet: stored (de-identified) but hidden
    assert count(analytics_db, "select count(*) from encounters_deid") == 1
    assert count(analytics_db, "select count(*) from v_encounters") == 0
    apply(analytics_db, deid, consent(True, 102))
    assert count(analytics_db, "select count(*) from v_encounters") == 1


def test_withdrawal_purges_everything_and_blocks_new_data(analytics_db, deid):
    lab = {"lab_id": 5, "encounter_id": 10, "patient_id": 1, "loinc_code": "2823-3",
           "test_name": "Potassium", "collected_at": T0}  # fmt: skip
    apply(analytics_db, deid, consent(True, 1), change("patients", "c", PATIENT, 100),
          change("encounters", "c", ENCOUNTER, 101),
          change("lab_results", "c", lab, 102))  # fmt: skip

    apply(analytics_db, deid, consent(False, 200))
    for table in ("patients_deid", "encounters_deid", "labs_deid"):
        assert count(analytics_db, f"select count(*) from {table}") == 0
    details = count(
        analytics_db, "select details from audit_log where action = 'consent_withdrawn_purge'"
    )
    assert details["rows_removed"] == {"labs_deid": 1, "encounters_deid": 1, "patients_deid": 1}

    later = apply(
        analytics_db, deid, change("encounters", "u", {**ENCOUNTER, "status": "discharged"}, 300)
    )
    assert later.held_no_consent == 1
    assert count(analytics_db, "select count(*) from encounters_deid") == 0


def test_regranted_consent_resumes_and_is_audited(analytics_db, deid):
    apply(analytics_db, deid, consent(True, 1), consent(False, 2), consent(True, 3),
          change("encounters", "c", ENCOUNTER, 100))  # fmt: skip
    assert count(analytics_db, "select count(*) from v_encounters") == 1
    assert (
        count(analytics_db, "select count(*) from audit_log where action = 'consent_granted'") == 1
    )


def test_stale_consent_event_is_ignored(analytics_db, deid):
    apply(analytics_db, deid, consent(True, 50), consent(False, 10))  # the withdrawal is older
    assert count(analytics_db, "select research_use from consent_state") is True


def test_unclassified_columns_are_dropped_and_audited(analytics_db, deid):
    apply(
        analytics_db,
        deid,
        change("patients", "c", {**PATIENT, "next_of_kin": "Jordan Rivera"}, 100),
    )
    entry = count(
        analytics_db, "select details from audit_log where action = 'unclassified_columns_dropped'"
    )
    assert entry == {"columns": ["next_of_kin"]}


def test_analytics_tables_hold_no_identifiers(analytics_db, deid):
    from src.leak_scan import scan

    apply(analytics_db, deid, consent(True, 1), change("patients", "c", PATIENT, 100),
          change("encounters", "c", ENCOUNTER, 101))  # fmt: skip
    assert scan(analytics_db) == []
    # The scanner itself works: plant an identifier and it is found (count only, no value)
    analytics_db.execute("update encounters_deid set primary_dx_code = '912-34-5678'")
    assert scan(analytics_db) == [{"table": "encounters_deid", "column": "primary_dx_code",
                                   "pattern": "ssn", "rows": 1}]  # fmt: skip


def test_analyst_login_sees_views_not_tables(analytics_db):
    psycopg = pytest.importorskip("psycopg")
    import uuid

    from conftest import TEST_DATABASE_URL
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    from src.analytics import apply_schema

    role = f"analyst_{uuid.uuid4().hex[:8]}"
    apply_schema(analytics_db, analyst_password="analyst-test", analyst_role=role)
    params = {**conninfo_to_dict(TEST_DATABASE_URL), "user": role, "password": "analyst-test"}
    try:
        with psycopg.connect(make_conninfo(**params), autocommit=True) as analyst:
            analyst.execute(f"set search_path to {analytics_db.schema}")
            assert analyst.execute("select count(*) from v_readmissions_30d").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                analyst.execute("select count(*) from encounters_deid")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                analyst.execute("select count(*) from audit_log")
    finally:
        analytics_db.execute(f"drop owned by {role}")
        analytics_db.execute(f"drop role {role}")

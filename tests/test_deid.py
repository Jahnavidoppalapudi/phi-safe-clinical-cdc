from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from conftest import TEST_KEY

from src.deid import Deidentifier, PolicyError, load_policy

PATIENT = {
    "patient_id": 17, "mrn": "MRN-12345678", "first_name": "Avery", "last_name": "Rivera",
    "date_of_birth": date(1961, 3, 14), "sex": "F", "ssn": "912-34-5678",
    "phone": "(512) 555-0142", "email": "avery.rivera7@example.org", "street": "12 Oak St",
    "city": "Austin", "state": "TX", "zip": "78701",
    "created_at": datetime(2026, 8, 1, 9, 30, tzinfo=UTC),
    "updated_at": datetime(2026, 8, 2, tzinfo=UTC),
}  # fmt: skip
DIRECT_IDENTIFIERS = {"mrn", "first_name", "last_name", "ssn", "phone", "email", "street", "city"}


def test_patient_row_keeps_no_direct_identifiers(deid):
    row, unclassified = deid.apply("patients", PATIENT)
    assert unclassified == []
    assert DIRECT_IDENTIFIERS.isdisjoint(row)
    assert set(map(str, row.values())).isdisjoint(str(PATIENT[c]) for c in DIRECT_IDENTIFIERS)
    assert row["patient_token"].startswith("pat_") and row["zip3"] == "787"
    assert (row["birth_year"], row["age_90_plus"], row["sex"], row["state"]) == (
        1961,
        False,
        "F",
        "TX",
    )


def test_tokens_are_stable_namespaced_and_keyed(deid, policy):
    assert deid.token("patient", 17) == deid.token("patient", 17)
    assert deid.token("patient", 17) != deid.token("encounter", 17)  # no cross-entity matching
    other_key = Deidentifier(b"z" * 32, policy)
    assert deid.token("patient", 17) != other_key.token("patient", 17)  # useless without the key
    assert deid.token("patient", None) is None


def test_date_shift_is_stable_nonzero_and_preserves_intervals(deid):
    shifts = {deid.shift_days(p) for p in range(2000)}
    assert 0 not in shifts and min(shifts) >= -365 and max(shifts) <= 365
    assert len(shifts) > 300  # spread out, not a handful of values
    admitted, discharged = datetime(2026, 3, 1, 8, tzinfo=UTC), datetime(2026, 3, 5, 17, tzinfo=UTC)
    enc = {
        "encounter_id": 1,
        "patient_id": 17,
        "admitted_at": admitted,
        "discharged_at": discharged,
    }
    out, _ = deid.apply("encounters", enc)
    assert out["admitted_at"] != admitted
    assert (
        out["discharged_at"] - out["admitted_at"] == discharged - admitted
    )  # length of stay intact


def test_same_patient_same_shift_across_tables(deid):
    lab = {"lab_id": 9, "encounter_id": 1, "patient_id": 17,
           "collected_at": datetime(2026, 3, 1, 9, tzinfo=UTC)}  # fmt: skip
    enc = {"encounter_id": 1, "patient_id": 17, "admitted_at": datetime(2026, 3, 1, 8, tzinfo=UTC)}
    lab_out, _ = deid.apply("lab_results", lab)
    enc_out, _ = deid.apply("encounters", enc)
    assert lab_out["collected_at"] - enc_out["admitted_at"] == datetime(2026, 3, 1, 9) - datetime(
        2026, 3, 1, 8
    )
    assert lab_out["encounter_token"] == enc_out["encounter_token"]


@pytest.mark.parametrize(
    ("dob", "expected"),
    [
        (date(1936, 11, 10), {"birth_year": 1936, "age_90_plus": False}),  # 89, birthday tomorrow
        (date(1936, 11, 9), {"birth_year": None, "age_90_plus": True}),  # turns 90 today
        (date(1920, 1, 1), {"birth_year": None, "age_90_plus": True}),
        (None, {"birth_year": None, "age_90_plus": None}),
    ],
)
def test_ages_90_and_over_are_aggregated(deid, dob, expected):
    assert deid.birth_year(dob) == expected


ZIPS = [("78701", "787"), ("05901", "000"), ("69201", "000"), ("89301", "000"),
        (None, None), ("12", None)]  # fmt: skip


@pytest.mark.parametrize(("zip_code", "zip3"), ZIPS)
def test_zip3_masks_sparsely_populated_areas(zip_code, zip3):
    assert Deidentifier.zip3(zip_code) == zip3


def test_unknown_columns_are_dropped_and_reported(deid):
    row, unclassified = deid.apply(
        "patients", {**PATIENT, "emergency_contact_phone": "(512) 555-0199"}
    )
    assert unclassified == ["emergency_contact_phone"]
    assert "(512) 555-0199" not in map(str, row.values())


def test_free_text_notes_never_pass(deid):
    enc = {"encounter_id": 1, "patient_id": 17, "notes": "Pt Avery Rivera, callback 555-0142",
           "attending_npi": "1234567890", "admitted_at": None}  # fmt: skip
    row, _ = deid.apply("encounters", enc)
    assert "notes" not in row and "attending_npi" not in row


def test_lab_values_survive_exactly(deid):
    lab = {"lab_id": 1, "encounter_id": 1, "patient_id": 17, "value_numeric": Decimal("5.123"),
           "loinc_code": "2823-3", "abnormal_flag": "H"}  # fmt: skip
    row, _ = deid.apply("lab_results", lab)
    assert (row["value_numeric"], row["loinc_code"], row["abnormal_flag"]) == (
        Decimal("5.123"),
        "2823-3",
        "H",
    )


def test_unknown_table_and_bad_policy_are_rejected(deid, tmp_path):
    with pytest.raises(PolicyError, match="not in the de-identification policy"):
        deid.apply("billing", {"patient_id": 1})
    bad = tmp_path / "p.yaml"
    bad.write_text("tables:\n  patients:\n    patient_id: token\n    ssn: hash\n")
    with pytest.raises(PolicyError, match="unknown action 'hash'"):
        load_policy(bad)
    bad.write_text("tables:\n  visits:\n    visit_id: token\n")
    with pytest.raises(PolicyError, match="needs patient_id"):
        load_policy(bad)


def test_short_keys_are_refused(policy):
    with pytest.raises(ValueError, match="at least 32 bytes"):
        Deidentifier(b"short", policy)
    assert Deidentifier(TEST_KEY, policy)

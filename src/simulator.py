"""Synthetic hospital activity written to the EHR database. No real people, no real PHI.

    python -m src.simulator

On first start it backfills 90 simulated days quickly (catch-up mode), then keeps running in
real time. Every change it makes (insert, update, delete) is what Debezium streams out.

Identifiers are synthetic by construction: SSNs use the 9xx area numbers that are never
issued, phone numbers use the 555-01xx range reserved for fiction, emails use example.org.
"""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

log = logging.getLogger("simulator")

FIRST_NAMES = ["Avery", "Jordan", "Riley", "Morgan", "Casey", "Quinn", "Parker", "Rowan", "Elena",
               "Marcus", "Priya", "Tomás", "Grace", "Ibrahim", "Mei", "Olivia", "Noah", "Amara",
               "Lucas", "Sofia", "Dmitri", "Hana", "Kwame", "Isla"]  # fmt: skip
LAST_NAMES = ["Rivera", "Okafor", "Lindqvist", "Nakamura", "Patel", "Novak", "Haddad", "Moreau",
              "Castillo", "Brennan", "Adeyemi", "Kowalski", "Fischer", "Tanaka", "Silva", "Duarte",
              "Hughes", "Ivanova", "Mbeki", "Larsen"]  # fmt: skip
# (city, state, ZIP). Includes ZIPs whose 3-digit prefix HIPAA Safe Harbor requires masking
PLACES = [("Springfield", "IL", "62704"), ("Austin", "TX", "78701"), ("Columbus", "OH", "43215"),
          ("Portland", "OR", "97205"), ("Raleigh", "NC", "27601"), ("Burlington", "VT", "05901"),
          ("Ogallala", "NE", "69201"), ("Tucson", "AZ", "85701"), ("Madison", "WI", "53703"),
          ("Elko", "NV", "89301")]  # fmt: skip
# department -> (encounter types, ICD-10-CM codes)
DEPARTMENTS = {
    "Cardiology": (["inpatient", "outpatient"], ["I21.4", "I50.9", "I48.91", "I10"]),
    "Pulmonology": (["inpatient", "outpatient"], ["J18.9", "J44.1", "J45.909"]),
    "Endocrinology": (["outpatient", "inpatient"], ["E11.65", "E11.9", "E87.1"]),
    "Orthopedics": (["inpatient", "outpatient"], ["S72.001A", "M17.11", "M54.5"]),
    "Neurology": (["inpatient", "outpatient"], ["I63.9", "G40.909", "G43.909"]),
    "Nephrology": (["inpatient", "outpatient"], ["N17.9", "N18.4"]),
    "Emergency": (["emergency"], ["R07.9", "R10.9", "S06.0X0A", "R55"]),
}
# LOINC code -> (name, unit, reference low, reference high)
LABS = {
    "2345-7": ("Glucose", "mg/dL", 70, 99),
    "2160-0": ("Creatinine", "mg/dL", 0.6, 1.3),
    "2823-3": ("Potassium", "mmol/L", 3.5, 5.1),
    "2951-2": ("Sodium", "mmol/L", 135, 145),
    "718-7": ("Hemoglobin", "g/dL", 12.0, 17.5),
    "6690-2": ("Leukocytes", "10*3/uL", 4.5, 11.0),
    "10839-9": ("Troponin I", "ng/mL", 0.0, 0.04),
}
# Length of stay in hours by encounter type: (shortest, longest, most common).
# Inpatient averages about 4.7 days, close to the US hospital average.
STAY_HOURS = {"inpatient": (24, 240, 78), "emergency": (2, 12, 4), "outpatient": (1, 3, 1.5)}


def planned_stay_hours(encounter_id: int, encounter_type: str) -> float:
    """Each stay gets its own planned length, fixed by its id so it is stable across restarts."""
    low, high, mode = STAY_HOURS[encounter_type]
    return random.Random(encounter_id).triangular(low, high, mode)


@dataclass
class Rates:
    """Average events per simulated hour."""

    new_patients: float = 0.8
    admissions: float = 1.2
    readmission_share: float = 0.25  # admissions drawn from recently discharged patients
    corrections: float = 0.05  # demographic corrections (address, phone)
    consent_changes: float = 0.03
    duplicate_removals: float = 0.01  # duplicate charts merged away
    cancellations: float = 0.02


@dataclass
class Simulator:
    conn: object
    rng: random.Random = field(default_factory=lambda: random.Random(42))
    rates: Rates = field(default_factory=Rates)

    # ------------------------------------------------------------------ helpers
    def _count(self, rate: float, hours: float) -> int:
        """Poisson-ish count of events for a time slice."""
        expected = rate * hours
        n = int(expected)
        return n + (1 if self.rng.random() < expected - n else 0)

    def _person(self) -> dict:
        r = self.rng
        first, last = r.choice(FIRST_NAMES), r.choice(LAST_NAMES)
        city, state, zip_code = r.choice(PLACES)
        age = min(int(r.triangular(0, 101, 62)), 101)  # skewed old; some 90+
        dob = date.today() - timedelta(days=age * 365 + r.randint(0, 364))
        return {
            "mrn": f"MRN-{r.randint(10**7, 10**8 - 1)}",
            "first_name": first, "last_name": last, "date_of_birth": dob,
            "sex": r.choice(["F", "M", "F", "M", "X", "U"]),
            "ssn": f"9{r.randint(0, 99):02d}-{r.randint(1, 99):02d}-{r.randint(1, 9999):04d}",
            "phone": f"({r.randint(200, 989)}) 555-01{r.randint(0, 99):02d}",
            "email": f"{first.lower()}.{last.lower()}{r.randint(1, 999)}@example.org",
            "street": f"{r.randint(1, 9999)} {r.choice(['Oak', 'Maple', 'Cedar', 'Elm'])} St",
            "city": city, "state": state, "zip": zip_code,
        }  # fmt: skip

    # ------------------------------------------------------------------ events
    def new_patient(self, at: datetime) -> int:
        p = self._person()
        with self.conn.cursor() as cur:
            pid = cur.execute(
                """insert into patients (mrn, first_name, last_name, date_of_birth, sex, ssn, phone,
                       email, street, city, state, zip, created_at, updated_at)
                   values (%(mrn)s, %(first_name)s, %(last_name)s, %(date_of_birth)s, %(sex)s,
                       %(ssn)s, %(phone)s, %(email)s, %(street)s, %(city)s, %(state)s, %(zip)s,
                       %(at)s, %(at)s)
                   on conflict (mrn) do nothing returning patient_id""",
                {**p, "at": at},
            ).fetchone()
            if pid is None:
                return 0
            cur.execute(
                "insert into consents (patient_id, research_use, updated_at) values (%s, %s, %s)",
                (pid[0], self.rng.random() < 0.9, at),
            )
        return pid[0]

    def admit(self, at: datetime) -> int | None:
        with self.conn.cursor() as cur:
            if self.rng.random() < self.rates.readmission_share:
                row = cur.execute(
                    """select patient_id from encounters where status = 'discharged'
                         and discharged_at > %s - interval '30 days'
                       order by random() limit 1""",
                    (at,),
                ).fetchone()
            else:
                # Patients not admitted or discharged in the last 30 days (no accidental readmits)
                row = cur.execute(
                    """select p.patient_id from patients as p
                       where not exists (
                           select 1 from encounters as e
                           where e.patient_id = p.patient_id
                             and coalesce(e.discharged_at, e.admitted_at) > %s - interval '30 days')
                       order by random() limit 1""",
                    (at,),
                ).fetchone()
            if row is None:
                return None
            department = self.rng.choice(list(DEPARTMENTS))
            types, codes = DEPARTMENTS[department]
            patient = cur.execute(
                "select first_name, last_name from patients where patient_id = %s", (row[0],)
            ).fetchone()
            # Free-text notes often repeat identifiers; that is why they never leave the PHI zone
            note = None
            if self.rng.random() < 0.5:
                callback = self._person()["phone"]
                note = f"Pt {patient[0]} {patient[1]} seen in {department}. Callback {callback}."
            enc = cur.execute(
                """insert into encounters (patient_id, encounter_type, department, admitted_at,
                       primary_dx_code, status, attending_npi, notes, updated_at)
                   values (%s, %s, %s, %s, %s, 'admitted', %s, %s, %s) returning encounter_id""",
                (row[0], self.rng.choice(types), department, at, self.rng.choice(codes),
                 f"1{self.rng.randint(10**8, 10**9 - 1)}", note, at),
            ).fetchone()[0]  # fmt: skip
            self._labs(cur, enc, row[0], at, n=self.rng.randint(1, 4))
        return enc

    def _labs(self, cur, encounter_id: int, patient_id: int, at: datetime, n: int) -> None:
        for loinc in self.rng.sample(list(LABS), k=n):
            name, unit, low, high = LABS[loinc]
            span = high - low or 0.1
            value = round(self.rng.gauss((low + high) / 2, span * 0.45), 3)
            value = max(value, 0)
            flag = "L" if value < low else "H" if value > high else "N"
            collected = at + timedelta(minutes=self.rng.randint(5, 90))
            cur.execute(
                """insert into lab_results (encounter_id, patient_id, loinc_code, test_name,
                       value_numeric, unit, reference_low, reference_high, abnormal_flag,
                       collected_at, resulted_at)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (encounter_id, patient_id, loinc, name, value, unit, low, high, flag,
                 collected, collected + timedelta(minutes=self.rng.randint(20, 240))),
            )  # fmt: skip

    def progress_stays(self, at: datetime) -> int:
        """Discharge patients whose stay is long enough; inpatients get daily labs."""
        discharged = 0
        with self.conn.cursor() as cur:
            open_stays = cur.execute(
                """select encounter_id, patient_id, encounter_type, admitted_at
                   from encounters where status = 'admitted' and admitted_at <= %s""",
                (at,),
            ).fetchall()
            for enc, pid, kind, admitted in open_stays:
                hours = (at - admitted).total_seconds() / 3600
                if hours >= planned_stay_hours(enc, kind):
                    cur.execute(
                        """update encounters set status = 'discharged', discharged_at = %s
                           where encounter_id = %s""",
                        (at, enc),
                    )
                    discharged += 1
                elif kind == "inpatient" and self.rng.random() < 0.08:
                    self._labs(cur, enc, pid, at, n=2)
        return discharged

    def correct_demographics(self) -> None:
        p = self._person()
        self.conn.execute(
            """update patients set street = %s, city = %s, state = %s, zip = %s, phone = %s
               where patient_id = (select patient_id from patients order by random() limit 1)""",
            (p["street"], p["city"], p["state"], p["zip"], p["phone"]),
        )

    def change_consent(self) -> None:
        self.conn.execute(
            """update consents set research_use = not research_use
               where patient_id = (select patient_id from consents order by random() limit 1)"""
        )

    def remove_duplicate_chart(self) -> None:
        """A duplicate chart is merged away: the patient row and everything under it is deleted."""
        self.conn.execute(
            """delete from patients where patient_id = (
                   select patient_id from patients order by random() limit 1)"""
        )

    def cancel_encounter(self) -> None:
        self.conn.execute(
            """update encounters set status = 'cancelled' where encounter_id = (
                   select encounter_id from encounters where status = 'admitted'
                   order by random() limit 1)"""
        )

    # ------------------------------------------------------------------ clock
    def step(self, at: datetime, hours: float) -> dict:
        """Simulate `hours` of activity ending at `at`, in one transaction."""
        counts = {"patients": 0, "admissions": 0, "discharges": 0}
        with self.conn.transaction():
            for _ in range(self._count(self.rates.new_patients, hours)):
                counts["patients"] += bool(self.new_patient(at))
            for _ in range(self._count(self.rates.admissions, hours)):
                counts["admissions"] += self.admit(at) is not None
            counts["discharges"] = self.progress_stays(at)
            for _ in range(self._count(self.rates.corrections, hours)):
                self.correct_demographics()
            for _ in range(self._count(self.rates.consent_changes, hours)):
                self.change_consent()
            for _ in range(self._count(self.rates.duplicate_removals, hours)):
                self.remove_duplicate_chart()
            for _ in range(self._count(self.rates.cancellations, hours)):
                self.cancel_encounter()
        return counts

    def resume_from(self, backfill_days: int) -> datetime:
        last = self.conn.execute("select max(admitted_at) from encounters").fetchone()[0]
        return last or datetime.now(UTC) - timedelta(days=backfill_days)


def main() -> None:
    import psycopg

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = psycopg.connect(os.environ["EHR_DATABASE_URL"], autocommit=True)
    sim = Simulator(conn, rng=random.Random(int(os.environ.get("SIM_SEED", "42"))))
    step_hours = float(os.environ.get("SIM_STEP_HOURS", "2"))
    clock = sim.resume_from(int(os.environ.get("SIM_BACKFILL_DAYS", "90")))
    log.info("simulated clock starts at %s", clock.isoformat(timespec="minutes"))

    while True:
        now = datetime.now(UTC)
        if clock + timedelta(hours=step_hours) < now:  # catching up on history
            clock += timedelta(hours=step_hours)
            sim.step(clock, step_hours)
            continue
        time.sleep(int(os.environ.get("SIM_LIVE_SECONDS", "10")))
        elapsed = (datetime.now(UTC) - clock).total_seconds() / 3600
        clock = datetime.now(UTC)
        log.info("live step: %s", sim.step(clock, elapsed))


if __name__ == "__main__":
    main()

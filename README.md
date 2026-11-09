# PHI-Safe Clinical CDC Pipeline

![CI](https://github.com/Jahnavidoppalapudi/phi-safe-clinical-cdc/actions/workflows/ci.yml/badge.svg)

> Streams every change from a (simulated) hospital EHR database with Debezium CDC, and de-identifies
> it before it reaches analytics: keyed pseudonyms, per-patient date shifting, HIPAA Safe Harbor
> rules for ages and ZIP codes, deny-by-default column policy, consent enforcement, and an audit trail.

**Status:** ✅ Complete end to end: hospital EHR → Debezium CDC → de-identification processor →
consent-gated analytics, with an audit trail and a continuous PHI leak scan.

All data is synthetic: SSNs use never-issued 9xx area numbers, phone numbers the reserved
555-01xx range, and email addresses `example.org`.

## The Problem

Hospitals want analytics on clinical data (readmissions, length of stay, lab trends) without exposing
who the patients are. That is harder than deleting a few columns:

- **Updates and deletes matter.** A corrected address, a cancelled admission, or a duplicate chart
  that is merged away must flow downstream, so batch exports and `updated_at` polling are not enough.
- **Dates are both identifying and essential.** Removing them (as HIPAA Safe Harbor requires) makes
  readmission analysis impossible; keeping them risks re-identification.
- **Free text leaks.** Clinical notes repeat names and phone numbers.
- **Schemas change.** A new column added to the EHR must not slip into analytics unreviewed.
- **Consent changes.** A patient who withdraws research consent must disappear from analytics.

## Architecture

```
                 PHI ZONE                                    │ BOUNDARY            │ ANALYTICS ZONE
                                                             │                     │
EHR PostgreSQL ──WAL──▶ Debezium ──▶ Redpanda ehr.public.* ──┼─▶ de-identification ┼─▶ analytics PostgreSQL
 wal_level=logical      (Kafka       raw PHI, 3-day          │   processor         │   tokens, shifted dates,
 4-table publication     Connect)    retention               │   policy · consent  │   no direct identifiers
 replica identity FULL                                       │   LSN ordering      │   ├─ v_* views (analyst)
        ▲                                                    │   audit · leak scan │   └─ audit_log (restricted)
 hospital simulator                                          │                     │
```

## How It Works

| Piece | What it does |
|---|---|
| **EHR schema** (`sql/ehr/`) | Patients, encounters, lab results (LOINC), consents. Full row images for CDC, a publication limited to four tables, a least-privilege replication login, WAL retention capped |
| **Simulator** (`src/simulator.py`) | Backfills 90 days, then runs live: admissions, labs, discharges, readmissions, corrections, consent changes, duplicate-chart removals |
| **Debezium CDC** | Connector registered as code (`src/register_connector.py`): idempotent, fails fast on a broken task |
| **De-identification** (`config/deid_policy.yaml`, `src/deid.py`) | Deny-by-default policy; HMAC pseudonyms; per-patient date shifts; birth year with 90+ grouping; ZIP3 with Safe Harbor masking; notes and provider IDs dropped |
| **Processor** (`src/processor.py`, `src/analytics.py`) | One analytics transaction per batch, then commit offsets. LSN-ordered upserts, delete tombstones, consent purge and blocking, audit entries |
| **Leak scan** (`src/leak_scan.py`) | Every 10 minutes, searches every text column for SSN, phone, email and MRN patterns; reports location and count, never the value |
| **Analyst access** | A login that can read only the consent-gated views: readmissions, length of stay, lab abnormality rates |

### Results on the simulated hospital

- 30-day inpatient readmission rate and average length of stay computed on the **de-identified,
  date-shifted** data are **identical** to the same metrics on the raw EHR data (tested in CI).
- Zero identifier patterns in the analytics zone (leak scan, tested in CI with a planted SSN to prove
  the scanner works).
- The simulator is calibrated to about a 4.7-day average inpatient stay and about a 13% 30-day
  readmission rate, close to US hospital averages. Its first version produced 2-day stays and 40%
  readmissions, because random patient selection kept re-admitting recently discharged patients;
  each stay now has its own planned length, and regular admissions skip recent patients.

## Key Design Decisions

- [ADR-001](docs/adr/001-cdc-with-debezium.md): log-based CDC with Debezium, and protecting the clinical database
- [ADR-002](docs/adr/002-de-identification.md): de-identification approach and its limits
- [ADR-003](docs/adr/003-phi-zones.md): separate PHI and analytics zones
- [ADR-004](docs/adr/004-consent-and-deletion.md): consent, deletion and LSN ordering in the analytics zone

## Testing

Tests run in CI against a real PostgreSQL 16 with logical replication enabled:

- **Policy vs schema contract:** every EHR column has a privacy decision; no stale rules.
- **De-identification:** no direct identifier survives; tokens are stable, namespaced and keyed;
  date shifts preserve intervals; the age-90 boundary; masked ZIP3 areas; unknown columns dropped.
- **Replication stream:** an update carries the full previous row; deleting a chart cascades delete
  events to all four tables.
- **Least privilege:** the CDC login can read and replicate but cannot modify data.
- **Simulator realism:** synthetic identifiers only; length of stay in a realistic range.
- **Analytics rules:** newer changes win; deleted rows cannot be resurrected by replays; withdrawal
  purges and blocks; unknown consent is hidden; the analyst login cannot read base tables or the audit log.
- **End to end:** 30 simulated days streamed as Debezium events through Kafka (mock broker) into the
  processor; a mid-stream consent withdrawal and chart deletion; metrics equal to the raw EHR; a full
  replay leaves the analytics zone unchanged.

## How to Run

Requires Docker Desktop and Python 3.12.

```bash
git clone git@github.com:Jahnavidoppalapudi/phi-safe-clinical-cdc.git
cd phi-safe-clinical-cdc
make env          # creates .env with random secrets
make setup        # local dev tools
make test         # unit tests
make up           # everything: PHI zone, processor, analytics zone
make connector    # Debezium connector status (should be RUNNING)
make plogs        # processor: applied, stale, held for consent, errors per batch
make metrics      # readmissions, length of stay, lab abnormality rates (de-identified)
make audit        # purges, deletions, unclassified columns, leak scans
make leak-scan    # search the analytics zone for identifier patterns now
make analyst      # SQL prompt with analyst permissions (views only)
make cdc-peek     # raw change events (synthetic PHI: this is the PHI zone)
make test-db      # all tests, including the database and replication tests
```

- Redpanda Console: http://localhost:8085 (topics and the Kafka Connect connector)

## What I'd Do in Production

- Key in a secrets manager (KMS / Vault), with key rotation planned as a full re-tokenization.
- Encryption at rest and TLS everywhere; Kafka ACLs so only Debezium and the processor can touch raw topics.
- A privacy officer's or expert's review of the policy, plus k-anonymity checks before any extract leaves.
- Debezium incremental snapshots (signal table) to restore a patient's history after consent is re-granted.
- Alerts on replication slot lag, retained WAL, processor lag, malformed-event rate, and any leak-scan finding.

## Data and Standards

- Diagnoses use ICD-10-CM codes; lab tests use LOINC codes with typical reference ranges.
- De-identification follows the methods described in HIPAA (45 CFR 164.514). This project
  demonstrates the engineering; it is not a compliance certification.

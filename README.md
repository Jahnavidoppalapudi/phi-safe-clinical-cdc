# PHI-Safe Clinical CDC Pipeline

![CI](https://github.com/Jahnavidoppalapudi/phi-safe-clinical-cdc/actions/workflows/ci.yml/badge.svg)

> Streams every change from a (simulated) hospital EHR database with Debezium CDC, and de-identifies
> it before it reaches analytics: keyed pseudonyms, per-patient date shifting, HIPAA Safe Harbor
> rules for ages and ZIP codes, deny-by-default column policy, consent enforcement, and an audit trail.

**Status:** 🚧 PHI zone complete: EHR simulator, Debezium CDC into Redpanda, and the de-identification
engine with its policy. The de-identification processor, consent enforcement and analytics zone are next.

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
                PHI ZONE                                       │          ANALYTICS ZONE (next)
                                                               │
EHR PostgreSQL ──WAL──▶ Debezium (Kafka Connect) ──▶ Redpanda  │
  wal_level=logical      pgoutput, least-privilege   ehr.public.*  ──▶ de-identification ──▶ analytics DB
  publication: 4 tables  login, heartbeats           3-day        │    processor             (tokens, shifted
  replica identity FULL                              retention    │    + consent + audit      dates, no PHI)
        ▲                                                      │
  hospital simulator                                           │
```

## What Is Built So Far

| Piece | What it does |
|---|---|
| **EHR schema** (`sql/ehr/`) | Patients, encounters, lab results (LOINC), consents. Full row images for CDC, a publication limited to four tables, a least-privilege replication login |
| **Simulator** (`src/simulator.py`) | Backfills 90 days, then runs live: admissions, labs, discharges, readmissions, corrections, consent changes, duplicate-chart removals. Calibrated to about 4.7-day inpatient stays and about 13% 30-day readmissions |
| **Debezium CDC** | Connector registered as code (`src/register_connector.py`), idempotent, fails fast on a broken task |
| **Change event parser** (`src/cdc.py`) | Inserts, snapshot reads, updates, deletes; dates, timestamps and exact decimals typed correctly |
| **De-identification engine** (`src/deid.py` + `config/deid_policy.yaml`) | Deny-by-default policy; HMAC pseudonyms; per-patient date shifts; birth year with 90+ grouping; ZIP3 with Safe Harbor masking |

## Key Design Decisions

- [ADR-001](docs/adr/001-cdc-with-debezium.md): log-based CDC with Debezium, and protecting the clinical database
- [ADR-002](docs/adr/002-de-identification.md): de-identification approach and its limits
- [ADR-003](docs/adr/003-phi-zones.md): separate PHI and analytics zones

## Testing

Tests run in CI against a real PostgreSQL 16 with logical replication enabled:

- **Policy vs schema contract:** every EHR column has a privacy decision; no stale rules.
- **De-identification:** no direct identifier survives; tokens are stable, namespaced and keyed;
  date shifts preserve intervals; the age-90 boundary; masked ZIP3 areas; unknown columns dropped.
- **Replication stream:** an update carries the full previous row; deleting a chart cascades delete
  events to all four tables.
- **Least privilege:** the CDC login can read and replicate but cannot modify data.
- **Simulator realism:** synthetic identifiers only; length of stay in a realistic range.

## How to Run

Requires Docker Desktop and Python 3.12.

```bash
git clone git@github.com:Jahnavidoppalapudi/phi-safe-clinical-cdc.git
cd phi-safe-clinical-cdc
make env          # creates .env with random secrets
make setup        # local dev tools
make test         # unit tests
make up           # EHR database, simulator, Redpanda, Debezium
make connector    # connector status (should be RUNNING)
make topics       # CDC topics with message counts
make cdc-peek     # raw change events (synthetic PHI: this is the PHI zone)
make test-db      # all tests, including the database and replication tests
```

- Redpanda Console: http://localhost:8085 (topics and the Kafka Connect connector)

## Data and Standards

- Diagnoses use ICD-10-CM codes; lab tests use LOINC codes with typical reference ranges.
- De-identification follows the methods described in HIPAA (45 CFR 164.514). This project
  demonstrates the engineering; it is not a compliance certification.

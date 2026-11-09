# ADR-003: Separate PHI and analytics zones

**Status:** Accepted
**Date:** 2026-11-09

## Context
Raw CDC events contain everything in the source row, including SSNs and full old rows on updates
and deletes. Wherever those events are stored becomes part of the protected environment.

## Decision
- **PHI zone:** the EHR database, Debezium, and the raw CDC topics (`ehr.public.*`). Raw topics keep
  data for **3 days** only, long enough to recover from a processor outage, not a second copy of
  the medical record.
- **Boundary:** the de-identification processor (Day 8) is the only consumer of raw topics. It applies
  the policy and writes only de-identified data out.
- **Analytics zone:** a separate database that never receives direct identifiers. Consent withdrawals
  are enforced there, and every privacy-relevant action is written to an audit log without PHI.

## Consequences
- Access control can be strict where it matters (raw topics, EHR) and relaxed for analysts.
- In production, raw topics would also need encryption at rest, TLS, and ACLs limiting them to the
  Debezium and processor service accounts; locally they are open for demonstration.

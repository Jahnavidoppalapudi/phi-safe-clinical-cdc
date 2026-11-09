# ADR-004: Consent, deletion and ordering in the analytics zone

**Status:** Accepted
**Date:** 2026-11-09

## Context
Change events arrive on four topics with no ordering between them, may be delivered more than once,
and may be replayed in full after an outage. Meanwhile patients withdraw (and occasionally re-grant)
research consent, and duplicate charts are deleted at the source. The analytics zone must end up in
the right state regardless of arrival order, and a patient who withdraws must disappear completely.

## Decisions
- **Order by the source's log position (LSN).** Every analytics row stores the LSN of the change that
  produced it; an update is applied only if its LSN is not older. Per row, PostgreSQL's LSN order is the
  commit order, so this is the true order of changes even across topics and partitions.
- **Remember deletions.** A deleted row leaves a tombstone (`deleted_rows`) with its LSN, so a replayed
  insert from before the deletion cannot bring it back.
- **Consent gates visibility, withdrawal purges.** Rows for patients whose consent is unknown or not
  given are stored de-identified but excluded from every analyst view, which avoids losing data when
  a consent event arrives after a patient's encounters. A withdrawal purges all of the patient's rows
  and blocks new ones. Re-granting resumes collection going forward; restoring the purged history is
  a deliberate re-snapshot, recorded in the audit log as needed.
- **Audit without PHI.** Purges (with row counts), source deletions, unclassified columns, malformed
  events (topic, partition and offset only) and leak-scan results go to `audit_log`. Malformed events
  are not copied out of the PHI zone; operators inspect them there, within the raw topics' 3-day retention.
- **Least privilege for analysts.** The `analyst` login can read the consent-gated `v_*` views only:
  not the base tables, not consent state, not the audit log.

## Consequences
- Replaying the entire stream is safe and is tested: the analytics zone is byte-for-byte unchanged.
- Interval metrics (readmissions, length of stay) are exact after date shifting; this is tested by
  comparing the de-identified result with the same metric computed on the raw EHR data. Calendar-date
  aggregates (admissions per real day) are deliberately not offered: per-patient shifts scramble them.
- A withdrawn patient's data can still exist in raw topics for up to 3 days, inside the PHI zone.

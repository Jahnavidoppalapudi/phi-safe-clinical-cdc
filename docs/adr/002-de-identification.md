# ADR-002: De-identification by policy: deny by default, keyed pseudonyms, per-patient date shifts

**Status:** Accepted
**Date:** 2026-11-09

## Context
Researchers need longitudinal analytics: readmissions, length of stay, lab trends per patient.
They must not see who the patients are. HIPAA describes two routes to de-identified data
(45 CFR 164.514): *Safe Harbor* (remove 18 identifier types, including all date elements except
year) and *Expert Determination* (a qualified expert finds the re-identification risk very small).
Pure Safe Harbor would remove admission dates, which makes readmission and length-of-stay analysis
impossible.

## Decision
A reviewed policy file (`config/deid_policy.yaml`) assigns every column one action:
- **Direct identifiers are dropped:** names, MRN, SSN, phone, email, street, city, provider NPI,
  and free-text notes (which, as the simulator shows, routinely repeat names and phone numbers).
- **Keys become keyed pseudonyms:** HMAC-SHA256 with a secret, namespaced per entity. Stable, so
  joins work; irreversible and not recomputable without the key; a patient token can never be
  matched to an encounter token.
- **Dates are shifted per patient** by a stable offset of 1–365 days (forward or back), derived
  from the same key. All of one patient's dates move together, so intervals (length of stay,
  days to readmission, lab timing) are exact, while calendar dates no longer match real events.
- **Safe Harbor rules where they apply:** birth year only, ages 90+ grouped with no year, ZIP
  reduced to 3 digits and replaced by `000` for the 17 sparsely populated 3-digit areas.
- **Deny by default:** a column missing from the policy is dropped and reported. A CI test checks
  that every column in the EHR schema has a decision, and no policy entry refers to a dropped column.

## Consequences
- This is closer to an expert-determination approach than to Safe Harbor, because shifted dates are
  kept. A real deployment needs a privacy officer's or expert's sign-off; this project documents the
  method, it does not certify compliance.
- The key is the most sensitive secret in the system: losing it breaks joins with earlier data;
  leaking it allows re-identification of tokens. It belongs in a secrets manager, with rotation
  planned as a full re-tokenization.
- Rare combinations (unusual diagnosis + small ZIP3 + birth year) can still be identifying; a
  production system would add k-anonymity checks on released extracts.

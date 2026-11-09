# ADR-001: Log-based change data capture with Debezium

**Status:** Accepted
**Date:** 2026-11-09

## Context
Analytics needs every change in the hospital database (new patients, admissions, lab results,
corrections, deleted duplicate charts, consent changes) within seconds, without slowing the
clinical system down and without missing deletes.

## Options Considered
1. **Nightly batch export:** simple, but a day late and blind to rows deleted between exports.
2. **Polling on `updated_at`:** misses deletes, misses rows updated twice between polls,
   depends on every writer maintaining the column, and adds query load to the clinical database.
3. **Triggers writing to an outbox table:** captures everything, but adds work to every clinical
   transaction and must be maintained per table.
4. **Log-based CDC (chosen):** Debezium reads PostgreSQL's write-ahead log through a logical
   replication slot. Every insert, update and delete, in commit order, with no extra query load.

## Decision
Debezium 3.6 PostgreSQL connector on Kafka Connect, `pgoutput` plugin, writing JSON to Redpanda.
- The **publication is created by the database owner** for exactly four tables, and Debezium may
  not create or widen it (`publication.autocreate.mode=disabled`).
- **`REPLICA IDENTITY FULL`** on every table, so updates and deletes carry the whole previous row.
  The processor needs `patient_id` from deleted rows to honour deletions downstream.
- **Least-privilege login** `cdc_reader`: replication plus SELECT on those four tables; not a superuser.
- **Initial snapshot** (`snapshot.mode=initial`) loads existing rows, then streaming continues from
  the slot's position with no gap.
- **Heartbeats every 10 s** keep the slot's confirmed position moving, and
  **`max_slot_wal_keep_size=1GB`** caps how much log a stalled slot can retain, so CDC problems
  can never fill the clinical database's disk.

## Consequences
- Deletes and every intermediate update are captured; ordering per row is preserved.
- A replication slot is a production dependency: it must be monitored (lag, retained WAL), and a
  connector that stays down longer than the WAL cap allows needs a fresh snapshot.

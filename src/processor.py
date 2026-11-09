"""The PHI boundary: consume raw CDC events, de-identify them, write the analytics zone.

    python -m src.processor

Each batch is applied in one analytics-database transaction, and Kafka offsets are committed
only after it succeeds, so a crash replays the batch and the LSN-ordered, idempotent writes in
src/analytics.py make the replay a no-op. Malformed events are not copied anywhere (the raw event
is PHI); an audit entry records where it is so an operator can inspect it inside the PHI zone.
"""

from __future__ import annotations

import logging
import os
import signal
import time

from src.analytics import ApplyResult, apply_event, audit
from src.cdc import parse_event

log = logging.getLogger("processor")
TOPICS = ["ehr.public.patients", "ehr.public.encounters", "ehr.public.lab_results",
          "ehr.public.consents"]  # fmt: skip
GROUP_ID = "deid-processor"
LEAK_SCAN_EVERY_SECONDS = 600


def consumer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    }


def process_batch(conn, messages, deid) -> tuple[ApplyResult, int]:
    result, errors = ApplyResult(), 0
    with conn.transaction(), conn.cursor() as cur:
        for msg in messages:
            if msg.error():
                errors += 1
                continue
            try:
                with conn.transaction():  # savepoint per event
                    event = parse_event(msg.value())
                    if event is not None:
                        apply_event(cur, event, deid, result)
            except Exception as err:
                errors += 1
                log.warning("event %s[%s]@%s rejected: %s", msg.topic(), msg.partition(),
                            msg.offset(), type(err).__name__)  # fmt: skip
                audit(cur, "malformed_event", None, msg.topic(), partition=msg.partition(),
                      offset=msg.offset(), error_type=type(err).__name__)  # fmt: skip
    return result, errors


def record_batch(conn, events: int, result: ApplyResult, errors: int, seconds: float) -> None:
    conn.execute(
        "insert into processor_batches values (now(), %s, %s, %s, %s, %s, %s) "
        "on conflict (batch_at) do nothing",
        (events, result.applied, result.stale_skipped, result.held_no_consent, errors,
         round(seconds, 3)),
    )  # fmt: skip


def run_leak_scan(conn) -> list[dict]:
    from src.leak_scan import scan

    findings = scan(conn)
    with conn.transaction(), conn.cursor() as cur:
        audit(cur, "leak_scan", findings=findings, clean=not findings)
    if findings:
        log.error("LEAK SCAN FOUND POSSIBLE PHI: %s", findings)
    return findings


def run(consumer, conn, deid, should_stop, max_batch=1000) -> None:
    last_scan = time.monotonic()
    while not should_stop():
        messages = consumer.consume(num_messages=max_batch, timeout=1.0)
        if messages:
            started = time.monotonic()
            result, errors = process_batch(conn, messages, deid)
            record_batch(conn, len(messages), result, errors, time.monotonic() - started)
            consumer.commit(asynchronous=False)  # only after the analytics transaction
            log.info("batch: %s events, %s applied, %s stale, %s held (no consent), %s errors",
                     len(messages), result.applied, result.stale_skipped, result.held_no_consent,
                     errors)  # fmt: skip
        if time.monotonic() - last_scan > LEAK_SCAN_EVERY_SECONDS:
            run_leak_scan(conn)
            last_scan = time.monotonic()


def load_key(hex_key: str) -> bytes:
    try:
        key = bytes.fromhex(hex_key)
    except ValueError as err:
        raise ValueError("DEID_SECRET must be hex (make env generates one)") from err
    if len(key) < 32:
        raise ValueError("DEID_SECRET must be at least 32 bytes (64 hex characters)")
    return key


def main() -> None:
    import psycopg
    from confluent_kafka import Consumer

    from src.analytics import apply_schema
    from src.deid import Deidentifier, load_policy

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    policy = load_policy(os.environ.get("DEID_POLICY", "config/deid_policy.yaml"))
    deid = Deidentifier(load_key(os.environ["DEID_SECRET"]), policy)
    conn = psycopg.connect(os.environ["ANALYTICS_DATABASE_URL"], autocommit=True)
    apply_schema(conn, os.environ.get("ANALYST_PASSWORD"))
    consumer = Consumer(consumer_config(os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")))
    consumer.subscribe(TOPICS)

    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(True))
    try:
        run(consumer, conn, deid, lambda: bool(stopping))
    finally:
        consumer.close()
        conn.close()
        log.info("processor stopped cleanly")


if __name__ == "__main__":
    main()

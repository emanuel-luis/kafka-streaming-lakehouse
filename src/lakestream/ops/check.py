"""End-to-end check: every layer through Trino, the DLQ from Kafka, against the manifests."""

import json
import time
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from typing import Any

import trino.dbapi
import trino.exceptions
from confluent_kafka import Consumer, KafkaException, TopicPartition

from lakestream.contracts import UUID_RE
from lakestream.ops.expectations import Expected, build_expected
from lakestream.settings import KafkaSettings, TrinoSettings

Query = Callable[[str], list[Any]]
# Missing tables or a Trino that is still starting are retried until the timeout.
RETRYABLE = (trino.exceptions.Error, trino.exceptions.HttpError, OSError, KafkaException)


@dataclass(frozen=True)
class Check:
    name: str
    expected: object
    actual: object
    ok: bool


def _eq(name: str, expected: object, actual: object) -> Check:
    return Check(name, expected, actual, expected == actual)


def _set_check(name: str, expected: set[Any], actual: list[Any]) -> Check:
    """Large sets print as sizes and differences, not as every row."""
    got = set(actual)
    dupes = len(actual) - len(got)
    missing, unexpected = len(expected - got), len(got - expected)
    summary = f"{len(actual)} rows, {missing} missing, {unexpected} unexpected, {dupes} duplicated"
    return Check(name, f"{len(expected)} rows", summary, not (missing or unexpected or dupes))


def dlq_coordinates(kafka: KafkaSettings, timeout: float = 30) -> set[tuple[int, int]]:
    """Distinct original (partition, offset) of the DLQ records for the source topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": f"check-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        meta = consumer.list_topics(kafka.dlq_topic, timeout=timeout).topics[kafka.dlq_topic]
        if meta.error is not None:
            raise KafkaException(meta.error)
        ends: dict[int, int] = {}
        starts = []
        for p in meta.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(kafka.dlq_topic, p), timeout=timeout
            )
            if high > low:
                ends[p] = high
                starts.append(TopicPartition(kafka.dlq_topic, p, low))
        consumer.assign(starts)
        found: set[tuple[int, int]] = set()
        deadline = time.monotonic() + timeout
        while ends and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            raw = msg.headers() or []
            pairs = raw.items() if isinstance(raw, dict) else raw
            headers = {k: v.decode() if isinstance(v, bytes) else v for k, v in pairs}
            if headers.get("dlq.original.topic") == kafka.topic:
                found.add(
                    (
                        int(headers["dlq.original.partition"] or -1),
                        int(headers["dlq.original.offset"] or -1),
                    )
                )
            if (msg.offset() or 0) + 1 >= ends.get(msg.partition() or 0, 0):
                ends.pop(msg.partition() or 0, None)
        if ends:
            raise KafkaException(f"DLQ not read to the end within {timeout}s: {ends}")
        return found
    finally:
        consumer.close()


def first_append_snapshot(q: Query) -> int | None:
    rows = q(
        "SELECT snapshot_id FROM bronze.\"events$snapshots\" WHERE operation = 'append'"
        " ORDER BY committed_at LIMIT 1"
    )
    return int(rows[0][0]) if rows else None


def run_checks(exp: Expected, q: Query, dlq: set[tuple[int, int]]) -> list[Check]:
    bronze, bronze_distinct = q(
        "SELECT count(*), (SELECT count(*) FROM (SELECT DISTINCT kafka_topic, kafka_partition,"
        " kafka_offset FROM bronze.events)) FROM bronze.events"
    )[0]
    (ingested,) = q(
        "SELECT coalesce(sum(CAST(element_at(summary, 'ingest.records') AS bigint)), 0)"
        " FROM bronze.\"events$snapshots\" WHERE operation = 'append'"
    )[0]
    rejects = Counter(dict(q("SELECT reject_reason, count(*) FROM silver.rejects GROUP BY 1")))
    silver, silver_distinct = q("SELECT count(*), count(DISTINCT event_id) FROM silver.events")[0]
    by_type = Counter(dict(q("SELECT event_type, count(*) FROM silver.events GROUP BY 1")))
    # Only canonical UUIDs are inlined into the SQL; anything else counts as missing.
    late_ids = ", ".join(f"'{i}'" for i in sorted(exp.late_event_ids) if UUID_RE.fullmatch(i))
    late_sql = f"SELECT count(DISTINCT event_id) FROM silver.events WHERE event_id IN ({late_ids})"
    present = q(late_sql)[0][0] if late_ids else 0
    dropped = q(
        "SELECT rows_dropped_late FROM ops.job_runs WHERE job = 'silver'"
        " ORDER BY finished_at DESC LIMIT 1"
    )
    minutes = [
        (ws.astimezone(UTC), t, n)
        for ws, t, n in q(
            "SELECT window_start, event_type, event_count FROM gold.events_per_minute"
        )
    ]
    orders, approved, amount = q(
        "SELECT sum(orders_placed), sum(payments_approved), sum(approved_amount)"
        " FROM gold.revenue_5min"
    )[0]
    sessions, session_events = q("SELECT count(*), sum(event_count) FROM gold.user_sessions")[0]
    expected_minutes = {(m, t, n) for (m, t), n in exp.minute_counts.items()}
    return [
        _eq("bronze rows", exp.bronze_rows, bronze),
        _eq("bronze distinct (topic, partition, offset)", bronze, bronze_distinct),
        _eq("bronze sum(ingest.records) over appends", bronze, ingested),
        _eq("dlq distinct original coordinates", exp.dlq_records, len(dlq)),
        _eq("rejects", exp.rejects, rejects.total()),
        _eq("rejects per reason", dict(exp.rejects_by_reason), dict(rejects)),
        _eq("silver rows", exp.silver_rows, silver),
        _eq("silver distinct event_id", exp.silver_rows, silver_distinct),
        _eq("silver per type", dict(exp.events_by_type), dict(by_type)),
        _eq("silver late ids present", len(exp.late_event_ids), present),
        _eq("silver last run rows_dropped_late", 0, dropped[0][0] if dropped else None),
        _set_check("gold events_per_minute rows", expected_minutes, minutes),
        _eq("gold orders_placed", exp.orders_placed, orders),
        _eq("gold payments_approved", exp.payments_approved, approved),
        _eq("gold approved_amount", exp.approved_amount, amount),
        _eq("gold sessions", exp.sessions, sessions),
        _eq("gold sum(event_count) = silver rows", silver, session_events),
    ]


def time_travel(q: Query) -> Check:
    """Bronze at its first append snapshot next to now: history only grows."""
    current = q("SELECT count(*) FROM bronze.events")[0][0]
    snapshot = first_append_snapshot(q)
    first = (
        q(f"SELECT count(*) FROM bronze.events FOR VERSION AS OF {snapshot}")[0][0]
        if snapshot is not None
        else None
    )
    name = f"bronze time travel: first append snapshot {snapshot} <= current"
    return Check(name, f"<= {current}", first, first is not None and first <= current)


def render(checks: Sequence[Check]) -> str:
    lines = []
    for c in checks:
        mark = "ok  " if c.ok else "FAIL"
        lines.append(f"{mark} {c.name}: expected {c.expected}, actual {c.actual}")
    failed = sum(not c.ok for c in checks)
    lines.append(f"{len(checks) - failed}/{len(checks)} checks passed")
    return "\n".join(lines)


def _attempt(
    exp: Expected, trino_cfg: TrinoSettings, kafka: KafkaSettings, with_time_travel: bool
) -> list[Check]:
    conn = trino.dbapi.Connection(
        host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, catalog=trino_cfg.catalog
    )
    with closing(conn):
        cur = conn.cursor()

        def q(sql: str) -> list[Any]:
            cur.execute(sql)
            return list(cur.fetchall())

        checks = run_checks(exp, q, dlq_coordinates(kafka))
        if with_time_travel:
            checks.append(time_travel(q))
        return checks


def run_check(
    *,
    manifests: Sequence[Path],
    trino_cfg: TrinoSettings,
    kafka: KafkaSettings,
    timeout: float,
    with_time_travel: bool,
    poll_seconds: float = 5.0,
) -> int:
    """Retry until every check passes or `timeout` passes; print the last result. 1 on failure."""
    exp = build_expected(json.loads(p.read_text("utf-8")) for p in manifests)
    deadline = time.monotonic() + timeout
    while True:
        try:
            checks = _attempt(exp, trino_cfg, kafka, with_time_travel)
            report = render(checks)
            ok = all(c.ok for c in checks)
        except RETRYABLE as exc:
            report, ok = f"FAIL query error: {type(exc).__name__}: {exc}", False
        if ok or time.monotonic() >= deadline:
            print(report)
            return 0 if ok else 1
        first_line = next((ln for ln in report.splitlines() if ln.startswith("FAIL")), report)
        print(f"not yet, retrying in {poll_seconds:.0f}s: {first_line}", flush=True)
        time.sleep(poll_seconds)

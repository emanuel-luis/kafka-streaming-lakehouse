"""`lakestream dlq`: print the oldest dead letters, their headers and a hex preview. Read-only."""

import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, TopicPartition

from lakestream.settings import KafkaSettings

PREVIEW_BYTES = 32


@dataclass(frozen=True)
class DlqRecord:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: Sequence[tuple[str, bytes | None]]


def hex_preview(value: bytes | None, limit: int = PREVIEW_BYTES) -> str:
    if value is None:
        return "null"
    more = f" ... ({len(value)} bytes)" if len(value) > limit else f" ({len(value)} bytes)"
    return value[:limit].hex(" ") + more


def format_record(rec: DlqRecord) -> str:
    key = "null" if rec.key is None else rec.key.decode("utf-8", errors="replace")
    lines = [f"partition {rec.partition} offset {rec.offset} key {key}"]
    for name, raw in rec.headers:
        text = "null" if raw is None else raw.decode("utf-8", errors="replace")
        lines.append(f"  {name}: {text}")
    lines.append(f"  value: {hex_preview(rec.value)}")
    return "\n".join(lines)


def read_dlq(kafka: KafkaSettings, limit: int, timeout: float = 30) -> list[DlqRecord]:
    """Up to `limit` records from the start of every DLQ partition. A throwaway group that
    never commits, and partitions are assigned (no subscribe), so no group state changes."""
    consumer = Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": f"dlq-show-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        meta = consumer.list_topics(kafka.dlq_topic, timeout=timeout).topics[kafka.dlq_topic]
        if meta.error is not None:
            raise KafkaException(meta.error)
        ends: dict[int, int] = {}
        starts = []
        for p in sorted(meta.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(kafka.dlq_topic, p), timeout=timeout
            )
            if high > low:
                ends[p] = high
                starts.append(TopicPartition(kafka.dlq_topic, p, low))
        consumer.assign(starts)
        out: list[DlqRecord] = []
        deadline = time.monotonic() + timeout
        while ends and len(out) < limit and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            partition, offset = msg.partition() or 0, msg.offset() or 0
            raw = msg.headers() or []
            pairs = raw.items() if isinstance(raw, dict) else raw
            headers = [(k, v.encode() if isinstance(v, str) else v) for k, v in pairs]
            out.append(DlqRecord(partition, offset, msg.key(), msg.value(), headers))
            if offset + 1 >= ends.get(partition, 0):
                ends.pop(partition, None)
        if ends and len(out) < limit:
            raise KafkaException(f"{kafka.dlq_topic} not read within {timeout}s: {ends}")
        return out
    finally:
        consumer.close()


def run_dlq(kafka: KafkaSettings, limit: int, timeout: float = 30) -> int:
    try:
        records = read_dlq(kafka, limit, timeout)
    except KafkaException as exc:
        print(f"cannot read {kafka.dlq_topic}: {exc}")
        return 1
    for rec in records:
        print(format_record(rec))
    print(f"{len(records)} record(s) shown from {kafka.dlq_topic} (limit {limit})")
    return 0

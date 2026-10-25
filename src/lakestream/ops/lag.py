"""Consumer group lag per partition: committed offset, end offset, lag and owner."""

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin._listoffsets import OffsetSpec


@dataclass(frozen=True)
class PartitionLag:
    partition: int
    committed: int | None
    end: int
    lag: int
    owner: str | None


def compute_lag(
    partitions: Sequence[int],
    committed: Mapping[int, int],
    end: Mapping[int, int],
    owners: Mapping[int, str],
) -> list[PartitionLag]:
    """A partition without a committed offset (absent or negative) lags by its whole length."""
    rows = []
    for p in sorted(partitions):
        c = committed.get(p)
        c = c if c is not None and c >= 0 else None
        rows.append(PartitionLag(p, c, end[p], max(end[p] - (c or 0), 0), owners.get(p)))
    return rows


def caught_up(rows: Sequence[PartitionLag]) -> bool:
    """Total lag is 0 and every partition has a committed offset, unless it is empty (end 0)."""
    return bool(rows) and all(r.lag == 0 and (r.committed is not None or r.end == 0) for r in rows)


def _committed(admin: AdminClient, group: str, topic: str) -> dict[int, int]:
    try:
        result = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[
            group
        ].result(30)
    except KafkaException:  # the group does not exist yet
        return {}
    return {tp.partition: tp.offset for tp in result.topic_partitions or [] if tp.topic == topic}


def _owners(admin: AdminClient, group: str, topic: str) -> dict[int, str]:
    try:
        desc = admin.describe_consumer_groups([group])[group].result(30)
    except KafkaException:
        return {}
    return {
        tp.partition: f"{m.client_id} ({m.member_id})"
        for m in desc.members
        for tp in (m.assignment.topic_partitions if m.assignment else [])
        if tp.topic == topic
    }


def read_lag(bootstrap_servers: str, group: str, topic: str) -> list[PartitionLag]:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    topics = admin.list_topics(topic, timeout=30).topics
    if topic not in topics or topics[topic].error is not None:
        raise RuntimeError(f"topic {topic} not found")
    partitions = sorted(topics[topic].partitions)
    latest = OffsetSpec.latest()  # type: ignore[no-untyped-call]
    futures = admin.list_offsets({TopicPartition(topic, p): latest for p in partitions})
    end = {tp.partition: f.result(30).offset for tp, f in futures.items()}
    return compute_lag(
        partitions, _committed(admin, group, topic), end, _owners(admin, group, topic)
    )


def render(group: str, topic: str, rows: Sequence[PartitionLag], as_json: bool) -> str:
    total = sum(r.lag for r in rows)
    if as_json:
        return json.dumps(
            {
                "group": group,
                "topic": topic,
                "total_lag": total,
                "partitions": [asdict(r) for r in rows],
            }
        )
    lines = [f"group={group} topic={topic} total_lag={total}"]
    lines.append(f"{'partition':>9} {'committed':>10} {'end':>10} {'lag':>8}  owner")
    for r in rows:
        committed = "-" if r.committed is None else str(r.committed)
        lines.append(f"{r.partition:>9} {committed:>10} {r.end:>10} {r.lag:>8}  {r.owner or '-'}")
    return "\n".join(lines)


def run_lag(
    *,
    bootstrap_servers: str,
    group: str,
    topic: str,
    wait: bool,
    timeout: float,
    as_json: bool,
    poll_seconds: float = 2.0,
) -> int:
    """Print the lag once; with `wait`, poll until `caught_up`. Returns 1 on timeout."""
    deadline = time.monotonic() + timeout
    while True:
        rows = read_lag(bootstrap_servers, group, topic)
        if not wait or caught_up(rows):
            print(render(group, topic, rows, as_json))
            return 0
        if time.monotonic() >= deadline:
            print(render(group, topic, rows, as_json))
            return 1
        time.sleep(poll_seconds)

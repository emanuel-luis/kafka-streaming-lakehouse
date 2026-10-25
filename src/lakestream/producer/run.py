"""Kafka I/O of `lakestream produce`. The pure parts live in generator.py and manifest.py."""

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from lakestream.contracts import SUBJECT
from lakestream.producer.generator import (
    GenParams,
    InjectParams,
    bad_magic_frame,
    generate,
    plan_sends,
    truncate_frame,
    with_device_types,
)
from lakestream.producer.manifest import build_manifest, write_manifest
from lakestream.producer.model import Event
from lakestream.registry import load_schema
from lakestream.runmeta import utc_now
from lakestream.settings import KafkaSettings

log = logging.getLogger("lakestream.produce")


@dataclass
class _Counters:
    delivered: int = 0
    failed: int = 0

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            if self.failed <= 5:
                log.error("delivery failed", extra={"error": str(err)})


def producer_config(bootstrap_servers: str) -> dict[str, str | bool | int]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
        "partitioner": "murmur2_random",
        "message.timeout.ms": 30_000,
    }


def run_produce(
    *,
    gen: GenParams,
    inj: InjectParams,
    rate: float,
    manifest_path: Path,
    kafka: KafkaSettings,
    registry_url: str,
    run_id: str,
    schema_version: int = 1,
) -> int:
    """Send the run. Returns the exit code: 1 on any failed delivery (no manifest then)."""
    started = utc_now()
    events = generate(gen)
    if schema_version == 2:
        events = with_device_types(events, gen.seed)
    plan = plan_sends(events, gen, inj)
    serializer = AvroSerializer(
        SchemaRegistryClient({"url": registry_url}),
        load_schema(schema_version),
        lambda event, _ctx: event.as_record(),
        # The schema must already be registered (its id is looked up). The subject is fixed so a
        # test topic can reuse the registered schema.
        {"auto.register.schemas": False, "subject.name.strategy": lambda _ctx, _name: SUBJECT},
    )
    ctx = SerializationContext(kafka.topic, MessageField.VALUE)

    def encode(kind: str, event: Event) -> bytes:
        if kind == "bad_magic":
            return bad_magic_frame(event)
        frame = serializer(event, ctx)
        assert frame is not None
        return truncate_frame(frame) if kind == "truncated" else frame

    counters = _Counters()
    producer = Producer(producer_config(kafka.bootstrap_servers))
    for item in plan.items:
        value = encode(item.kind, item.event)
        key = item.key.encode("utf-8")
        while True:
            try:
                producer.produce(
                    kafka.topic, key=key, value=value, on_delivery=counters.on_delivery
                )
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        if rate > 0:
            time.sleep(1 / rate)
    remaining = producer.flush(60)
    counters.failed += remaining
    finished = utc_now()

    summary = {
        "records": len(plan.items),
        "delivered": counters.delivered,
        "failed": counters.failed,
        "seconds": round((finished - started).total_seconds(), 3),
    }
    if counters.failed:
        log.error("produce failed", extra=summary)
        return 1
    manifest = build_manifest(
        plan=plan,
        run_id=run_id,
        seed=gen.seed,
        schema_version=schema_version,
        base_time=gen.base_time,
        span_minutes=gen.span_minutes,
        sessions=gen.sessions,
        started_at=started,
        finished_at=finished,
        delivered=counters.delivered,
        failed=counters.failed,
    )
    write_manifest(manifest_path, manifest)
    log.info("produce finished", extra={**summary, "manifest": str(manifest_path)})
    return 0

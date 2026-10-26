import contextlib
import json
import os
import socket
import subprocess
import sys
import uuid
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from lakestream.lake import load_lake_catalog
from lakestream.ops.lag import read_lag
from lakestream.producer.generator import GenParams, InjectParams
from lakestream.producer.run import run_produce
from lakestream.registry import register_schema
from lakestream.settings import (
    IcebergSettings,
    KafkaSettings,
    S3Settings,
    SchemaRegistrySettings,
)

pytestmark = pytest.mark.integration

_CLI = "from lakestream.cli import app; app()"


def _require(hostport: str) -> None:
    host, _, port = hostport.partition(":")
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
    except OSError:
        pytest.fail(f"{hostport} is unreachable: start the stack with `make up` first")


def _consume(env: dict[str, str], *args: str, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _CLI, "consume", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _dlq_coordinates(bootstrap: str, dlq_topic: str, source_topic: str) -> list[tuple[int, int]]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"it-dlq-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([dlq_topic])
    found: list[tuple[int, int]] = []
    idle = 0
    while idle < 15:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            idle += 1
            continue
        idle = 0
        headers = {k: v for k, v in msg.headers() or [] if v is not None}
        assert headers["dlq.consumer.group"], "DLQ record without its consumer group header"
        assert headers["dlq.error.class"], "DLQ record without its error class header"
        if headers["dlq.original.topic"].decode() == source_topic:
            found.append(
                (int(headers["dlq.original.partition"]), int(headers["dlq.original.offset"]))
            )
    consumer.close()
    return found


def test_consumer_crash_and_restart(tmp_path: Path) -> None:
    kafka, registry = KafkaSettings(), SchemaRegistrySettings()
    iceberg, s3 = IcebergSettings(), S3Settings()
    for hostport in (
        kafka.bootstrap_servers,
        urlparse(registry.url).netloc,
        urlparse(iceberg.rest_uri).netloc,
        urlparse(s3.endpoint).netloc,
    ):
        _require(hostport)
    register_schema(registry.url)

    name = f"it_{uuid.uuid4().hex}"
    topic, dlq_topic, group = name, f"{name}.dlq", f"group-{name}"
    table_id = f"bronze.{name}"
    kafka = kafka.model_copy(update={"topic": topic, "dlq_topic": dlq_topic})
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})
    created = admin.create_topics(
        [NewTopic(topic, num_partitions=6, replication_factor=1)]
        + [NewTopic(dlq_topic, num_partitions=1, replication_factor=1)]
    )
    for future in created.values():
        future.result(30)

    catalog = load_lake_catalog(iceberg, s3)
    try:
        manifest_path = tmp_path / "manifest.json"
        code = run_produce(
            gen=GenParams(sessions=60),
            inj=InjectParams(),
            rate=0,
            manifest_path=manifest_path,
            kafka=kafka,
            registry_url=registry.url,
            run_id="it-consumer",
        )
        assert code == 0
        produced = json.loads(manifest_path.read_text("utf-8"))["produced"]
        assert produced["avro"] >= 100 and produced["malformed"] > 0

        env = {
            **os.environ,
            "KAFKA_TOPIC": topic,
            "KAFKA_DLQ_TOPIC": dlq_topic,
            "CONSUMER_GROUP_ID": group,
            "CONSUMER_BRONZE_TABLE": table_id,
            "CONSUMER_BATCH_MAX_MESSAGES": "50",
            "CONSUMER_BATCH_MAX_SECONDS": "2",
            "CONSUMER_HEARTBEAT_FILE": str(tmp_path / "heartbeat"),
        }
        crashed = _consume(env, "--crash-after-write-batch", "2", timeout=240)
        assert crashed.returncode == 1, f"expected the crash hook:\n{crashed.stderr[-3000:]}"
        table = catalog.load_table(table_id)
        assert len(table.scan().to_arrow()) > 0, "nothing was written before the crash"

        restarted = _consume(env, "--exit-when-idle", "15", timeout=300)
        assert restarted.returncode == 0, restarted.stderr[-3000:]

        rows = catalog.load_table(table_id).scan().to_arrow()
        coords = list(
            zip(
                rows["kafka_partition"].to_pylist(),
                rows["kafka_offset"].to_pylist(),
                strict=True,
            )
        )
        assert len(coords) == produced["avro"]
        duplicated = [c for c, n in Counter(coords).items() if n > 1]
        assert not duplicated, f"duplicate coordinates after the restart: {duplicated[:5]}"
        assert set(rows["kafka_topic"].to_pylist()) == {topic}

        dlq = _dlq_coordinates(kafka.bootstrap_servers, dlq_topic, topic)
        assert len(set(dlq)) == produced["malformed"]

        assert sum(r.lag for r in read_lag(kafka.bootstrap_servers, group, topic)) == 0
    finally:
        with contextlib.suppress(Exception):  # cleanup only; the table may not exist
            catalog.drop_table(table_id)
        admin.delete_topics([topic, dlq_topic])

import json
import socket
import uuid
from pathlib import Path
from urllib.parse import urlparse

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

from lakestream.contracts import SUBJECT
from lakestream.producer.generator import GenParams, InjectParams
from lakestream.producer.run import run_produce
from lakestream.registry import register_schema
from lakestream.settings import KafkaSettings, SchemaRegistrySettings

pytestmark = pytest.mark.integration


def _require(hostport: str) -> None:
    host, _, port = hostport.partition(":")
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
    except OSError:
        pytest.fail(f"{hostport} is unreachable: start the stack with `make up` first")


def test_produce_round_trip(tmp_path: Path) -> None:
    kafka, registry = KafkaSettings(), SchemaRegistrySettings()
    _require(kafka.bootstrap_servers)
    _require(urlparse(registry.url).netloc)
    register_schema(registry.url)
    topic = f"it_{uuid.uuid4().hex}"
    kafka = kafka.model_copy(update={"topic": topic})
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})
    admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=1)])[topic].result(30)

    manifest_path = tmp_path / "m.json"
    code = run_produce(
        gen=GenParams(sessions=20),
        inj=InjectParams(),
        rate=0,
        manifest_path=manifest_path,
        kafka=kafka,
        registry_url=registry.url,
        run_id="it",
    )
    assert code == 0
    total = json.loads(manifest_path.read_text("utf-8"))["produced"]["total"]

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": f"it-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    seen = idle = 0
    while seen < total and idle < 30:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            idle += 1
        else:
            seen += 1
            idle = 0
    consumer.close()
    assert seen == total

    client = SchemaRegistryClient({"url": registry.url})
    assert SUBJECT in client.get_subjects()
    assert client.get_compatibility(SUBJECT) == "BACKWARD"

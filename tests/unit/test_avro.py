import io
import json
from typing import Any

import fastavro
import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from lakestream.contracts import DEVICE_TYPES
from lakestream.producer.generator import (
    GenParams,
    InjectParams,
    generate,
    plan_sends,
    with_device_types,
)
from lakestream.producer.manifest import build_manifest
from lakestream.registry import is_compatible, load_schema, read_schema
from lakestream.runmeta import utc_now


def _round_trip(version: int, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    schema = fastavro.parse_schema(json.loads(load_schema(version)))
    out = []
    for record in records:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, record)
        buf.seek(0)
        out.append(fastavro.schemaless_reader(buf, schema, schema))
    return out


def test_round_trip() -> None:
    schema = fastavro.parse_schema(json.loads(load_schema(1)))
    assert schema["name"] == "lakehouse.shop.ShopEvent"
    records = [e.as_record() for e in generate(GenParams(sessions=30))]
    assert _round_trip(1, records) == records


def test_v2_adds_only_an_optional_device_type() -> None:
    v1, v2 = json.loads(load_schema(1)), json.loads(load_schema(2))
    assert v2["fields"][:-1] == v1["fields"]
    assert v2["fields"][-1] == {"name": "device_type", "type": ["null", "string"], "default": None}
    breaking = json.loads(read_schema("shop_event.v2-breaking.avsc"))
    assert "default" not in breaking["fields"][-1] and breaking["fields"][-1]["type"] == "string"


def test_v2_events_carry_a_seeded_device_per_session() -> None:
    p = GenParams(sessions=40, seed=43)
    v1 = generate(p)
    v2 = with_device_types(v1, p.seed)
    assert v2 == with_device_types(generate(p), p.seed)  # deterministic
    assert [e.event_id for e in v2] == [e.event_id for e in v1]
    assert all(e.device_type in DEVICE_TYPES for e in v2)
    by_session = {(e.session_id, e.device_type) for e in v2}
    assert len(by_session) == len({e.session_id for e in v2})
    records = [e.as_record() for e in v2]
    assert _round_trip(2, records) == records
    assert "device_type" not in v1[0].as_record()  # v1 records are unchanged


def test_manifest_counts_device_type_events() -> None:
    p = GenParams(sessions=20, users=10, seed=43)
    now = utc_now()

    def manifest(version: int) -> dict[str, Any]:
        events = generate(p) if version == 1 else with_device_types(generate(p), p.seed)
        plan = plan_sends(events, p, InjectParams())
        return build_manifest(
            plan=plan, run_id="r", seed=p.seed, schema_version=version, base_time=p.base_time,
            span_minutes=p.span_minutes, sessions=p.sessions, started_at=now, finished_at=now,
            delivered=0, failed=0,
        )  # fmt: skip

    m1, m2 = manifest(1), manifest(2)
    assert (m1["schema_version"], m1["device_type_events"]) == (1, 0)
    assert (m2["schema_version"], m2["device_type_events"]) == (2, m2["clean_events"])
    assert m2["clean_events"] == m1["clean_events"]


def test_read_schema_by_path_or_packaged_name(tmp_path: Any) -> None:
    f = tmp_path / "x.avsc"
    f.write_text('{"type": "string"}', "utf-8")
    assert read_schema(f) == '{"type": "string"}'
    assert read_schema("shop_event.v2.avsc") == load_schema(2)
    with pytest.raises(FileNotFoundError):
        read_schema("nope.avsc")


class _Registry:
    def __init__(self, answer: bool | int) -> None:
        self.answer = answer

    def test_compatibility(self, _subject: str, _schema: object) -> bool:
        if isinstance(self.answer, bool):
            return self.answer
        raise SchemaRegistryError(self.answer, 40401 if self.answer == 404 else 50001, "x")


def test_is_compatible() -> None:
    schema = load_schema(2)
    assert is_compatible(_Registry(True), schema)  # type: ignore[arg-type]
    assert not is_compatible(_Registry(False), schema)  # type: ignore[arg-type]
    assert is_compatible(_Registry(404), schema)  # type: ignore[arg-type]  # no version yet
    with pytest.raises(SchemaRegistryError):
        is_compatible(_Registry(500), schema)  # type: ignore[arg-type]

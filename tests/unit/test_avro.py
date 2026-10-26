import io
import json

import fastavro

from lakestream.producer.generator import GenParams, generate
from lakestream.registry import load_schema


def test_round_trip() -> None:
    schema = fastavro.parse_schema(json.loads(load_schema(1)))
    assert schema["name"] == "lakehouse.shop.ShopEvent"
    for event in generate(GenParams(sessions=30)):
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, event.as_record())
        buf.seek(0)
        out = fastavro.schemaless_reader(buf, schema, schema)
        assert out == event.as_record()

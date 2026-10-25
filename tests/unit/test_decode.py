from datetime import UTC, datetime
from typing import Any

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext
from hypothesis import given
from hypothesis import strategies as st

from lakestream.consumer.batch import Rec
from lakestream.consumer.decode import (
    AVRO_DECODE,
    NOT_FRAMED,
    NULL_VALUE,
    UNKNOWN_SCHEMA,
    DecodeFailure,
    decode,
    parse_header,
)
from lakestream.producer.generator import GenParams, bad_magic_frame, generate, truncate_frame
from lakestream.registry import load_schema

TOPIC = "shop.events"
CTX = SerializationContext(TOPIC, MessageField.VALUE)


@pytest.fixture(scope="module")
def registry() -> SchemaRegistryClient:
    return SchemaRegistryClient.new_client({"url": "mock://decode-tests"})


@pytest.fixture(scope="module")
def frame(registry: SchemaRegistryClient) -> bytes:
    event = generate(GenParams(sessions=2))[0]
    serializer = AvroSerializer(
        registry,
        load_schema(1),
        lambda e, _ctx: e.as_record(),
        {"auto.register.schemas": True, "subject.name.strategy": lambda _c, _n: "s-value"},
    )
    out = serializer(event, CTX)
    assert out is not None
    return out


def rec(value: bytes | None) -> Rec:
    return Rec(TOPIC, 0, 7, 1_000, b"k", value)


def test_valid_frame_decodes(registry: SchemaRegistryClient, frame: bytes) -> None:
    schema_id, record = decode(AvroDeserializer(registry), rec(frame))
    assert schema_id == int.from_bytes(frame[1:5], "big")
    assert record["event_id"]
    assert isinstance(record["event_time"], datetime) and record["event_time"].tzinfo == UTC


def test_truncated_body_is_a_decode_error(registry: SchemaRegistryClient, frame: bytes) -> None:
    with pytest.raises(DecodeFailure) as exc:
        decode(AvroDeserializer(registry), rec(truncate_frame(frame)))
    assert exc.value.error_class == AVRO_DECODE


def test_bad_magic_and_short_are_not_framed(registry: SchemaRegistryClient) -> None:
    event = generate(GenParams(sessions=2))[0]
    for value in (bad_magic_frame(event), b"\x00\x00", b""):
        with pytest.raises(DecodeFailure) as exc:
            decode(AvroDeserializer(registry), rec(value))
        assert exc.value.error_class == NOT_FRAMED


def test_null_value(registry: SchemaRegistryClient) -> None:
    with pytest.raises(DecodeFailure) as exc:
        decode(AvroDeserializer(registry), rec(None))
    assert exc.value.error_class == NULL_VALUE


def test_unknown_schema_id_is_permanent(registry: SchemaRegistryClient, frame: bytes) -> None:
    unknown = b"\x00" + (999_999).to_bytes(4, "big") + frame[5:]
    with pytest.raises(DecodeFailure) as exc:
        decode(AvroDeserializer(registry), rec(unknown))
    assert exc.value.error_class == UNKNOWN_SCHEMA


def _raising(exc: Exception) -> Any:
    def deserializer(_value: bytes, _ctx: SerializationContext) -> Any:
        raise exc

    return deserializer


def test_registry_5xx_is_transient(frame: bytes) -> None:
    err = SchemaRegistryError(503, 50300, "unavailable")
    with pytest.raises(SchemaRegistryError):
        decode(_raising(err), rec(frame))


def test_registry_connection_error_is_transient(frame: bytes) -> None:
    class ConnectError(Exception):
        __module__ = "httpx2"

    with pytest.raises(ConnectError):
        decode(_raising(ConnectError("refused")), rec(frame))
    with pytest.raises(ConnectionRefusedError):
        decode(_raising(ConnectionRefusedError()), rec(frame))


@pytest.mark.parametrize(
    "exc", [EOFError(), ValueError("x"), UnicodeDecodeError("u", b"", 0, 1, "x")]
)
def test_other_errors_are_decode_errors(frame: bytes, exc: Exception) -> None:
    with pytest.raises(DecodeFailure) as failure:
        decode(_raising(exc), rec(frame))
    assert failure.value.error_class == AVRO_DECODE


@given(st.binary(max_size=40))
def test_parse_header_total(value: bytes) -> None:
    if len(value) > 5 and value[0] == 0:
        assert parse_header(value) == int.from_bytes(value[1:5], "big")
    else:
        with pytest.raises(DecodeFailure) as exc:
            parse_header(value)
        assert exc.value.error_class == NOT_FRAMED

"""Confluent wire-format decoding and error classification.

Permanent failures (the record can never decode) raise `DecodeFailure` and go to the DLQ.
Transient failures (registry unreachable or answering 5xx) propagate unchanged so the caller
retries the batch; such records never reach the DLQ.
"""

from collections.abc import Callable
from typing import Any

from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

from lakestream.consumer.batch import Rec

# error classes written to the `dlq.error.class` header
NULL_VALUE = "NullValue"
NOT_FRAMED = "NotConfluentFramed"
UNKNOWN_SCHEMA = "UnknownSchemaId"
AVRO_DECODE = "AvroDecodeError"

Deserializer = Callable[[bytes, SerializationContext], Any]


class DecodeFailure(Exception):
    """A record that is permanently undecodable."""

    def __init__(self, error_class: str, message: str) -> None:
        super().__init__(message)
        self.error_class = error_class
        self.message = message


def parse_header(value: bytes | None) -> int:
    """Schema id of a Confluent-framed value (magic byte 0, 4-byte big-endian id, body)."""
    if value is None:
        raise DecodeFailure(NULL_VALUE, "record value is null")
    if len(value) <= 5 or value[0] != 0:
        raise DecodeFailure(NOT_FRAMED, f"not Confluent-framed Avro ({len(value)} bytes)")
    return int.from_bytes(value[1:5], "big")


def _is_transport_error(exc: Exception) -> bool:
    # The registry client's HTTP library is a private dependency of confluent-kafka: match by
    # module name instead of importing it.
    return isinstance(exc, OSError) or type(exc).__module__.split(".")[0].startswith("httpx")


def decode(deserializer: Deserializer, rec: Rec) -> tuple[int, dict[str, Any]]:
    """Return (schema_id, record), or raise DecodeFailure (permanent) / the transient error."""
    schema_id = parse_header(rec.value)
    assert rec.value is not None
    ctx = SerializationContext(rec.topic, MessageField.VALUE)
    try:
        record = deserializer(rec.value, ctx)
    except SchemaRegistryError as exc:
        if exc.http_status_code == 404:
            raise DecodeFailure(
                UNKNOWN_SCHEMA, f"schema id {schema_id} not in the registry"
            ) from exc
        raise  # 5xx and the like: transient
    except Exception as exc:
        if _is_transport_error(exc):
            raise
        raise DecodeFailure(AVRO_DECODE, f"{type(exc).__name__}: {exc}") from exc
    if not isinstance(record, dict):
        raise DecodeFailure(AVRO_DECODE, f"top-level Avro value is {type(record).__name__}")
    return schema_id, record

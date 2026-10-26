"""Avro schema files and Schema Registry registration."""

from importlib.resources import files

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from lakestream.contracts import SUBJECT

COMPATIBILITY = "BACKWARD"


def load_schema(version: int = 1) -> str:
    return files("lakestream").joinpath("schemas", f"shop_event.v{version}.avsc").read_text("utf-8")


def register_schema(url: str, version: int = 1, subject: str = SUBJECT) -> int:
    """Set BACKWARD compatibility on the subject, then register. Idempotent: same id each time."""
    client = SchemaRegistryClient({"url": url})
    client.set_compatibility(subject_name=subject, level=COMPATIBILITY)
    return client.register_schema(subject, Schema(load_schema(version), "AVRO"))

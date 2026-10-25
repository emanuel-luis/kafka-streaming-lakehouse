"""Avro schema files and Schema Registry registration."""

from importlib.resources import files
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from lakestream.contracts import SUBJECT

COMPATIBILITY = "BACKWARD"
SCHEMA_VERSIONS = (1, 2)


class IncompatibleSchemaError(Exception):
    """The registry says the schema breaks the subject's compatibility rule."""


def load_schema(version: int = 1) -> str:
    return read_schema(f"shop_event.v{version}.avsc")


def read_schema(path: str | Path) -> str:
    """A schema file by path, or by the name of a schema shipped in the package."""
    if Path(path).is_file():
        return Path(path).read_text("utf-8")
    packaged = files("lakestream").joinpath("schemas", Path(path).name)
    if not packaged.is_file():
        raise FileNotFoundError(f"no schema file {path} (nor a packaged schema of that name)")
    return packaged.read_text("utf-8")


def is_compatible(client: SchemaRegistryClient, schema: str, subject: str = SUBJECT) -> bool | None:
    """Ask the compatibility endpoint. None: the subject has no version yet, so anything goes."""
    try:
        return client.test_compatibility(subject, Schema(schema, "AVRO"))
    except SchemaRegistryError as exc:
        if exc.http_status_code == 404:
            return None
        raise


def check_schema(url: str, schema: str, subject: str = SUBJECT) -> bool | None:
    return is_compatible(SchemaRegistryClient({"url": url}), schema, subject)


def register_schema(url: str, version: int = 1, subject: str = SUBJECT) -> int:
    """Set BACKWARD on the subject, test compatibility, then register. Same id each time."""
    client = SchemaRegistryClient({"url": url})
    client.set_compatibility(subject_name=subject, level=COMPATIBILITY)
    schema = load_schema(version)
    if is_compatible(client, schema, subject) is False:
        raise IncompatibleSchemaError(
            f"v{version} is not {COMPATIBILITY} compatible with {subject}"
        )
    return client.register_schema(subject, Schema(schema, "AVRO"))

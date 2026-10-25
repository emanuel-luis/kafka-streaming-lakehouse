"""Bronze table: Avro to Iceberg types, table creation, atomic appends and the offset ledger.

Every append writes the Kafka offsets it holds into the snapshot summary, so the table itself says
what it contains (decision 14): a crash between the Iceberg commit and the Kafka commit cannot
write a batch twice.
"""

import json
import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from lakestream.consumer.batch import BACKOFF_SECONDS, Rec

log = logging.getLogger("lakestream.bronze")

TABLE_PROPERTIES = {"format-version": "2", "write.parquet.compression-codec": "zstd"}

# (operation, additional summary properties), newest snapshot first.
Summaries = Iterable[tuple[str, Mapping[str, str]]]


# --- Avro -> Iceberg --------------------------------------------------------------------------


def avro_to_iceberg(avro_type: Any) -> IcebergType:
    """Iceberg type of an Avro type; a union with null maps to the type of its other branch."""
    if isinstance(avro_type, list):
        branches = [t for t in avro_type if t != "null"]
        if len(branches) != 1:
            raise ValueError(f"unsupported Avro union: {avro_type}")
        return avro_to_iceberg(branches[0])
    if isinstance(avro_type, str):
        avro_type = {"type": avro_type}
    base, logical = avro_type["type"], avro_type.get("logicalType")
    if not isinstance(base, str):
        return avro_to_iceberg(base)
    match base, logical:
        case "string", _:
            return StringType()
        case "int", _:
            return IntegerType()
        case "long", "timestamp-millis" | "timestamp-micros":
            return TimestamptzType()
        case "long", _:
            return LongType()
        case "float", _:
            return FloatType()
        case "double", _:
            return DoubleType()
        case "boolean", _:
            return BooleanType()
        case "bytes", "decimal":
            return DecimalType(avro_type["precision"], avro_type.get("scale", 0))
        case "bytes", _:
            return BinaryType()
    raise ValueError(f"unsupported Avro type: {avro_type}")


def avro_fields(avro_schema: Mapping[str, Any], first_id: int = 1) -> list[NestedField]:
    """Every Avro field as an OPTIONAL column: bronze accepts any writer schema the registry did."""
    return [
        NestedField(first_id + i, f["name"], avro_to_iceberg(f["type"]), required=False)
        for i, f in enumerate(avro_schema["fields"])
    ]


def bronze_schema(avro_schema_json: str) -> Schema:
    """Avro fields first, then the Kafka and ingest columns (required, except a null key)."""
    fields = avro_fields(json.loads(avro_schema_json))
    n = len(fields)
    meta: list[tuple[str, IcebergType, bool]] = [
        ("kafka_topic", StringType(), True),
        ("kafka_partition", IntegerType(), True),
        ("kafka_offset", LongType(), True),
        ("kafka_timestamp", TimestamptzType(), True),
        ("kafka_key", StringType(), False),
        ("schema_id", IntegerType(), True),
        ("ingested_at", TimestamptzType(), True),
        ("run_id", StringType(), True),
        ("batch_id", LongType(), True),
    ]
    fields += [
        NestedField(n + 1 + i, name, t, required=req) for i, (name, t, req) in enumerate(meta)
    ]
    return Schema(*fields)


def ensure_table(catalog: Catalog, identifier: str, schema: Schema) -> Table:
    """Create the namespace and table when missing; two consumers may start at the same time."""
    catalog.create_namespace_if_not_exists(Catalog.namespace_from(identifier))
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        pass
    spec = PartitionSpec(
        PartitionField(
            source_id=schema.find_field("ingested_at").field_id,
            field_id=1000,
            transform=DayTransform(),
            name="ingested_at_day",
        )
    )
    try:
        return catalog.create_table(
            identifier, schema, partition_spec=spec, properties=TABLE_PROPERTIES
        )
    except TableAlreadyExistsError:
        return catalog.load_table(identifier)


# --- rows -------------------------------------------------------------------------------------


def build_row(
    rec: Rec,
    schema_id: int,
    record: Mapping[str, Any],
    *,
    run_id: str,
    batch_id: int,
    ingested_at: datetime,
) -> dict[str, Any]:
    """The Avro record plus the Kafka coordinates and ingest metadata (which win on a clash)."""
    ts = datetime.fromtimestamp(rec.timestamp_ms / 1000, ingested_at.tzinfo)
    return {
        **record,
        "kafka_topic": rec.topic,
        "kafka_partition": rec.partition,
        "kafka_offset": rec.offset,
        "kafka_timestamp": ts if rec.timestamp_ms > 0 else ingested_at,
        "kafka_key": None if rec.key is None else rec.key.decode("utf-8", errors="replace"),
        "schema_id": schema_id,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "batch_id": batch_id,
    }


def to_arrow(table: Table, rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    """Arrow batch with exactly the table types. Keys the table lacks are not carried."""
    return pa.Table.from_pylist(list(rows), schema=table.schema().as_arrow())


# --- snapshot summaries -----------------------------------------------------------------------


def build_summary(
    *,
    run_id: str,
    batch_id: int,
    group: str,
    topic: str,
    offsets: Mapping[int, int],
    records: int,
    dlq_records: int,
) -> dict[str, str]:
    return {
        "ingest.run_id": run_id,
        "ingest.batch_id": str(batch_id),
        "ingest.consumer_group": group,
        "ingest.topic": topic,
        "ingest.offsets": json.dumps({str(p): o for p, o in sorted(offsets.items())}),
        "ingest.records": str(records),
        "ingest.dlq_records": str(dlq_records),
    }


def read_summaries(table: Table) -> list[tuple[str, Mapping[str, str]]]:
    """Newest first, from the metadata the table already holds (no refresh)."""
    out: list[tuple[str, Mapping[str, str]]] = []
    for snap in reversed(table.metadata.snapshots):
        if snap.summary is not None:
            out.append((snap.summary.operation.value, snap.summary.additional_properties))
    return out


def snapshot_summaries(table: Table) -> list[tuple[str, Mapping[str, str]]]:
    """Newest first. The table is refreshed so commits of other instances are seen."""
    return read_summaries(table.refresh())


def walk_last_offsets(summaries: Summaries, group: str, topic: str) -> dict[int, int]:
    """Highest offset written per partition by this group on this topic (append snapshots only)."""
    last: dict[int, int] = {}
    for operation, props in summaries:
        if (
            operation != "append"
            or props.get("ingest.consumer_group") != group
            or props.get("ingest.topic") != topic
        ):
            continue
        for partition, offset in json.loads(props.get("ingest.offsets", "{}")).items():
            p = int(partition)
            last[p] = max(last.get(p, offset), offset)
    return last


def last_written_offsets(table: Table, group: str, topic: str) -> dict[int, int]:
    # Walks the whole history; snapshot expiry (`lakestream maintain`) bounds the cost.
    return walk_last_offsets(snapshot_summaries(table), group, topic)


def find_batch(summaries: Summaries, run_id: str, batch_id: int) -> bool:
    return any(
        op == "append"
        and props.get("ingest.run_id") == run_id
        and props.get("ingest.batch_id") == str(batch_id)
        for op, props in summaries
    )


def has_batch(table: Table, run_id: str, batch_id: int) -> bool:
    """True when a snapshot carries this run and batch: the outcome of an unknown commit."""
    return find_batch(snapshot_summaries(table), run_id, batch_id)


# --- append -----------------------------------------------------------------------------------


class OffsetFenceError(Exception):
    """Another writer of this group already wrote offsets this batch holds (a zombie writer)."""


def overlapping_partitions(
    written: Mapping[int, int], first_offsets: Mapping[int, int]
) -> dict[int, int]:
    """Partitions where the table already holds an offset of the batch: written >= first."""
    return {p: w for p, w in written.items() if p in first_offsets and w >= first_offsets[p]}


def append_batch(
    table: Table,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, str],
    *,
    first_offsets: Mapping[int, int],
    retries: int,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Append rows in one snapshot, retrying with the 1, 2, 4, 8, 16 s backoff.

    A lost commit race (CommitFailedException) refreshes and retries at once. After any other
    failure the outcome may be unknown, so the table is searched for this run and batch first.

    Before each append the refreshed table is checked for offsets of this group and topic at or
    past `first_offsets`; a hit raises OffsetFenceError, never retried. The append asserts the
    parent snapshot, so the check is atomic with the commit (decision 21).
    """
    run_id, batch_id = summary["ingest.run_id"], int(summary["ingest.batch_id"])
    for attempt in range(retries + 1):
        try:
            if attempt and has_batch(table, run_id, batch_id):
                log.info("append already committed", extra={"batch_id": batch_id})
                return
            table = table.refresh()
            written = walk_last_offsets(
                read_summaries(table), summary["ingest.consumer_group"], summary["ingest.topic"]
            )
            clash = overlapping_partitions(written, first_offsets)
            if clash:
                raise OffsetFenceError(
                    f"batch {batch_id} fenced, offsets already written: "
                    + ", ".join(
                        f"partition {p} written up to {w}, batch starts at {first_offsets[p]}"
                        for p, w in sorted(clash.items())
                    )
                )
            table.append(to_arrow(table, rows), snapshot_properties=dict(summary))
            return
        except OffsetFenceError:
            raise
        except CommitFailedException:
            if attempt == retries:
                raise
            log.warning("commit conflict, retrying", extra={"batch_id": batch_id})
        except Exception as exc:
            if attempt == retries:
                raise
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            log.warning(
                "append failed, retrying",
                extra={
                    "batch_id": batch_id,
                    "attempt": attempt + 1,
                    "delay": delay,
                    "error": str(exc),
                },
            )
            sleep(delay)

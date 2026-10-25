"""Bronze writes against a real (local SQLite + file warehouse) Iceberg catalog, no network.

The REST catalog of the stack is not needed to exercise table creation, partitioned appends
(the `days(ingested_at)` transform) and the snapshot-summary bookkeeping.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.types import StringType

from lakestream.consumer import bronze
from lakestream.consumer.batch import Rec
from lakestream.consumer.bronze import (
    FENCE_PROPERTY,
    TABLE_PROPERTIES,
    OffsetFenceError,
    append_batch,
    avro_fields,
    bronze_schema,
    build_row,
    build_summary,
    ensure_table,
    evolve_schema,
    has_batch,
    last_written_offsets,
)
from lakestream.contracts import DEVICE_TYPES
from lakestream.producer.generator import GenParams, generate, with_device_types
from lakestream.registry import load_schema

TOPIC = "shop.events"


def _catalog(tmp_path: Path) -> SqlCatalog:
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        warehouse=tmp_path.as_uri(),
        # PyArrowFileIO cannot open `file:///D:/...` locations on Windows; fsspec can.
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )


def test_partitioned_appends_and_offset_bookkeeping(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    schema = bronze_schema(load_schema(1))
    table = ensure_table(catalog, "bronze.events", schema)
    ensure_table(catalog, "bronze.events", schema)  # a second instance starting: no error

    events = generate(GenParams(sessions=5))[:10]
    ingested_at = datetime(2026, 1, 1, 12, tzinfo=UTC)
    for batch_id, chunk in enumerate((events[:6], events[6:]), start=1):
        base = 0 if batch_id == 1 else 6
        rows = [
            build_row(
                Rec(TOPIC, i % 2, base + i, 1_767_268_800_000, e.user_id.encode(), b""),
                1,
                e.as_record(),
                run_id="run-a",
                batch_id=batch_id,
                ingested_at=ingested_at,
            )
            for i, e in enumerate(chunk)
        ]
        offsets: dict[int, int] = {}
        for row in rows:
            offsets[row["kafka_partition"]] = row["kafka_offset"]
        summary = build_summary(
            run_id="run-a",
            batch_id=batch_id,
            group="g",
            topic=TOPIC,
            offsets=offsets,
            records=len(rows),
            dlq_records=0,
        )
        first = {0: base, 1: base + 1}
        append_batch(table, rows, summary, first_offsets=first, retries=0)

    scanned = catalog.load_table("bronze.events").scan().to_arrow()
    assert scanned.num_rows == 10
    assert sorted(scanned["kafka_offset"].to_pylist()) == list(range(10))
    assert last_written_offsets(table, "g", TOPIC) == {0: 8, 1: 9}
    assert last_written_offsets(table, "other-group", TOPIC) == {}
    assert has_batch(table, "run-a", 2)
    assert not has_batch(table, "run-a", 3)


def test_offset_fence_on_a_real_table(tmp_path: Path) -> None:
    table = ensure_table(_catalog(tmp_path), "bronze.events", bronze_schema(load_schema(1)))
    events = generate(GenParams(sessions=5))[:10]
    ingested_at = datetime(2026, 1, 1, 12, tzinfo=UTC)

    def append(
        first: int, last: int, *, batch_id: int, group: str = "g", topic: str = TOPIC
    ) -> None:
        rows = [
            build_row(
                Rec(topic, 0, o, 1_767_268_800_000, None, b""),
                1,
                events[o % 10].as_record(),
                run_id="run-z",
                batch_id=batch_id,
                ingested_at=ingested_at,
            )
            for o in range(first, last + 1)
        ]
        summary = build_summary(
            run_id="run-z",
            batch_id=batch_id,
            group=group,
            topic=topic,
            offsets={0: last},
            records=len(rows),
            dlq_records=0,
        )
        append_batch(table, rows, summary, first_offsets={0: first}, retries=0)

    def snapshots() -> int:
        return len(table.refresh().metadata.snapshots)

    append(0, 9, batch_id=1)
    assert snapshots() == 1
    with pytest.raises(OffsetFenceError):
        append(5, 14, batch_id=2)
    assert snapshots() == 1
    append(0, 4, batch_id=3, group="other")  # a different group is not fenced
    append(0, 4, batch_id=4, topic="other.topic")  # nor a different topic
    append(10, 19, batch_id=5)
    assert snapshots() == 4


def test_ensure_table_sets_the_fence_property(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    schema = bronze_schema(load_schema(1))
    props = {k: v for k, v in TABLE_PROPERTIES.items() if k != FENCE_PROPERTY}
    catalog.create_namespace("bronze")
    catalog.create_table("bronze.events", schema, properties=props)
    assert FENCE_PROPERTY not in catalog.load_table("bronze.events").properties
    table = ensure_table(catalog, "bronze.events", schema)
    assert table.properties[FENCE_PROPERTY] == "0"
    assert catalog.load_table("bronze.events").properties[FENCE_PROPERTY] == "0"


def test_conflict_between_fence_and_append_reaches_the_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = _catalog(tmp_path)
    table = ensure_table(catalog, "bronze.events", bronze_schema(load_schema(1)))
    other = catalog.load_table("bronze.events")
    events = generate(GenParams(sessions=5))[:10]
    at = datetime(2026, 1, 1, 12, tzinfo=UTC)

    def batch(run: str) -> tuple[list[dict[str, Any]], dict[str, str]]:
        rows = [
            build_row(
                Rec(TOPIC, 0, o, 1_767_268_800_000, None, b""),
                1,
                events[o].as_record(),
                run_id=run,
                batch_id=1,
                ingested_at=at,
            )
            for o in range(10)
        ]
        return rows, build_summary(
            run_id=run, batch_id=1, group="g", topic=TOPIC, offsets={0: 9}, records=10,
            dlq_records=0,
        )  # fmt: skip

    real_to_arrow = bronze.to_arrow
    calls: list[int] = []

    def racing_to_arrow(t: Any, rows: Any) -> Any:
        if not calls:  # the zombie passed the fence; now the other writer commits the same offsets
            calls.append(1)
            append_batch(other, *batch("run-b"), first_offsets={0: 0}, retries=0)
        return real_to_arrow(t, rows)

    monkeypatch.setattr(bronze, "to_arrow", racing_to_arrow)
    with pytest.raises(OffsetFenceError):
        append_batch(table, *batch("run-a"), first_offsets={0: 0}, retries=5)
    scanned = catalog.load_table("bronze.events").scan().to_arrow()
    assert scanned["run_id"].to_pylist() == ["run-b"] * 10


def _v2_batch(
    first: int, n: int, *, version: int, run: str = "run-e", batch_id: int = 1
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    p = GenParams(sessions=5, seed=43)
    events = generate(p) if version == 1 else with_device_types(generate(p), p.seed)
    rows = [
        build_row(
            Rec(TOPIC, 0, o, 1_767_268_800_000, None, b""),
            version,
            events[o % len(events)].as_record(),
            run_id=run,
            batch_id=batch_id,
            ingested_at=datetime(2026, 1, 2, 12, tzinfo=UTC),
        )
        for o in range(first, first + n)
    ]
    summary = build_summary(
        run_id=run, batch_id=batch_id, group="g", topic=TOPIC, offsets={0: first + n - 1},
        records=n, dlq_records=0,
    )  # fmt: skip
    return rows, summary


V2_FIELDS = avro_fields(json.loads(load_schema(2)))


def test_append_evolves_the_schema_for_a_new_writer_schema(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)
    table = ensure_table(catalog, "bronze.events", bronze_schema(load_schema(1)))
    rows, summary = _v2_batch(0, 4, version=1)
    append_batch(table, rows, summary, first_offsets={0: 0}, retries=0)
    rows, summary = _v2_batch(4, 4, version=2, batch_id=2)
    append_batch(table, rows, summary, first_offsets={0: 4}, retries=0, writer_fields=V2_FIELDS)

    loaded = catalog.load_table("bronze.events")
    assert loaded.schema().find_field("device_type").field_type == StringType()
    assert not loaded.schema().find_field("device_type").required
    schemas = len(loaded.metadata.schemas)
    rows, summary = _v2_batch(8, 2, version=2, batch_id=3)
    append_batch(table, rows, summary, first_offsets={0: 8}, retries=0, writer_fields=V2_FIELDS)
    assert len(catalog.load_table("bronze.events").metadata.schemas) == schemas  # no-op

    scanned = catalog.load_table("bronze.events").scan().to_arrow().sort_by("kafka_offset")
    devices = scanned["device_type"].to_pylist()
    assert devices[:4] == [None] * 4  # v1 rows
    assert all(d in DEVICE_TYPES for d in devices[4:])
    assert scanned["schema_id"].to_pylist() == [1] * 4 + [2] * 6


def test_schema_race_with_the_other_replica_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    catalog = _catalog(tmp_path)
    table = ensure_table(catalog, "bronze.events", bronze_schema(load_schema(1)))
    other = catalog.load_table("bronze.events")
    stale = catalog.load_table("bronze.events")
    evolve_schema(other, V2_FIELDS)
    with pytest.raises(CommitFailedException):  # bronze never retries inside PyIceberg
        evolve_schema(stale, V2_FIELDS)

    (tmp_path / "second").mkdir()
    catalog = _catalog(tmp_path / "second")
    table = ensure_table(catalog, "bronze.events", bronze_schema(load_schema(1)))
    other = catalog.load_table("bronze.events")
    real = bronze.evolve_schema
    calls: list[int] = []

    def racing(t: Any, fields: Any) -> Any:
        if not calls:  # the other replica adds the column right after this one refreshed
            calls.append(1)
            real(other, fields)
        return real(t, fields)

    monkeypatch.setattr(bronze, "evolve_schema", racing)
    rows, summary = _v2_batch(0, 3, version=2)
    append_batch(table, rows, summary, first_offsets={0: 0}, retries=1, writer_fields=V2_FIELDS)
    assert "commit conflict, retrying" in caplog.messages
    loaded = catalog.load_table("bronze.events")
    assert [f.name for f in loaded.schema().fields].count("device_type") == 1
    assert loaded.scan().to_arrow().num_rows == 3


def test_fence_reads_the_metadata_of_the_schema_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Why the schema update runs before the fence: a schema commit asserts schema ids, not the
    snapshot, so it succeeds over another writer's append and returns metadata holding it."""
    catalog = _catalog(tmp_path)
    table = ensure_table(catalog, "bronze.events", bronze_schema(load_schema(1)))
    other = catalog.load_table("bronze.events")
    real = bronze.evolve_schema
    calls: list[int] = []

    def racing(t: Any, fields: Any) -> Any:
        if not calls:  # after this writer refreshed, the other writes the same offsets
            calls.append(1)
            append_batch(other, *_v2_batch(0, 5, version=1, run="run-b"), first_offsets={0: 0},
                         retries=0)  # fmt: skip
        return real(t, fields)

    monkeypatch.setattr(bronze, "evolve_schema", racing)
    rows, summary = _v2_batch(0, 5, version=2, run="run-a")
    with pytest.raises(OffsetFenceError):
        append_batch(table, rows, summary, first_offsets={0: 0}, retries=0, writer_fields=V2_FIELDS)
    loaded = catalog.load_table("bronze.events")
    assert loaded.schema().find_field("device_type") is not None  # the schema commit landed
    assert loaded.scan().to_arrow()["run_id"].to_pylist() == ["run-b"] * 5

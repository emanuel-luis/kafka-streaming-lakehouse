"""Bronze writes against a real (local SQLite + file warehouse) Iceberg catalog, no network.

The REST catalog of the stack is not needed to exercise table creation, partitioned appends
(the `days(ingested_at)` transform) and the snapshot-summary bookkeeping.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from lakestream.consumer.batch import Rec
from lakestream.consumer.bronze import (
    OffsetFenceError,
    append_batch,
    bronze_schema,
    build_row,
    build_summary,
    ensure_table,
    has_batch,
    last_written_offsets,
)
from lakestream.producer.generator import GenParams, generate
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

"""Bronze writes against a real (local SQLite + file warehouse) Iceberg catalog, no network.

The REST catalog of the stack is not needed to exercise table creation, partitioned appends
(the `days(ingested_at)` transform) and the snapshot-summary bookkeeping.
"""

from datetime import UTC, datetime
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog

from lakestream.consumer.batch import Rec
from lakestream.consumer.bronze import (
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
        append_batch(table, rows, summary, retries=0)

    scanned = catalog.load_table("bronze.events").scan().to_arrow()
    assert scanned.num_rows == 10
    assert sorted(scanned["kafka_offset"].to_pylist()) == list(range(10))
    assert last_written_offsets(table, "g", TOPIC) == {0: 8, 1: 9}
    assert last_written_offsets(table, "other-group", TOPIC) == {}
    assert has_batch(table, "run-a", 2)
    assert not has_batch(table, "run-a", 3)

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestamptzType,
)

from lakestream.consumer.batch import Rec
from lakestream.consumer.bronze import (
    OffsetFenceError,
    append_batch,
    avro_to_iceberg,
    bronze_schema,
    build_row,
    build_summary,
    find_batch,
    overlapping_partitions,
    walk_last_offsets,
)
from lakestream.registry import load_schema


def test_avro_type_mapping() -> None:
    assert avro_to_iceberg("string") == StringType()
    assert avro_to_iceberg(["null", "int"]) == IntegerType()
    assert avro_to_iceberg({"type": "long"}) == LongType()
    assert avro_to_iceberg({"type": "long", "logicalType": "timestamp-millis"}) == TimestamptzType()
    decimal = {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}
    assert avro_to_iceberg(["null", decimal]) == DecimalType(12, 2)
    with pytest.raises(ValueError):
        avro_to_iceberg(["null", "string", "int"])
    with pytest.raises(ValueError):
        avro_to_iceberg({"type": "record", "name": "x", "fields": []})


def test_bronze_schema_v1() -> None:
    schema = bronze_schema(load_schema(1))
    avro = [f for f in schema.fields if f.name not in _META]
    assert len(avro) == 14 and all(not f.required for f in avro)
    meta = {f.name: f for f in schema.fields if f.name in _META}
    assert set(meta) == _META
    assert all(f.required for n, f in meta.items() if n != "kafka_key")
    assert schema.find_field("unit_price").field_type == DecimalType(10, 2)
    assert schema.find_field("event_time").field_type == TimestamptzType()
    assert len({f.field_id for f in schema.fields}) == len(schema.fields)


_META = {
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_key",
    "schema_id",
    "ingested_at",
    "run_id",
    "batch_id",
}


def test_row_converts_to_the_table_arrow_schema() -> None:
    schema = bronze_schema(load_schema(1))
    now = datetime(2026, 1, 1, 12, tzinfo=UTC)
    record = {
        "event_id": "e1",
        "event_type": "add_to_cart",
        "event_time": now,
        "user_id": "u",
        "session_id": "s",
        "quantity": 2,
        "unit_price": Decimal("9.99"),
    }
    row = build_row(
        Rec("shop.events", 4, 77, 1_767_268_800_000, b"u", b""),
        3,
        record,
        run_id="r",
        batch_id=9,
        ingested_at=now,
    )
    assert row["kafka_timestamp"] == now and row["kafka_key"] == "u"
    table = pa.Table.from_pylist([row], schema=schema.as_arrow())
    assert table.num_rows == 1
    assert table.schema.field("unit_price").type == pa.decimal128(10, 2)
    assert str(table.schema.field("event_time").type) == "timestamp[us, tz=UTC]"
    assert table.schema.field("kafka_offset").nullable is False


def test_build_row_null_key_and_missing_timestamp() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    row = build_row(Rec("t", 0, 0, -1, None, b""), 1, {}, run_id="r", batch_id=1, ingested_at=now)
    assert row["kafka_key"] is None and row["kafka_timestamp"] == now


def test_summary_properties() -> None:
    summary = build_summary(
        run_id="r",
        batch_id=2,
        group="g",
        topic="shop.events",
        offsets={3: 30, 0: 5},
        records=10,
        dlq_records=1,
    )
    assert summary == {
        "ingest.run_id": "r",
        "ingest.batch_id": "2",
        "ingest.consumer_group": "g",
        "ingest.topic": "shop.events",
        "ingest.offsets": '{"0": 5, "3": 30}',
        "ingest.records": "10",
        "ingest.dlq_records": "1",
    }


def snap(op: str, group: str, topic: str, offsets: dict[int, int], run: str = "r", batch: int = 1):
    props = build_summary(
        run_id=run, batch_id=batch, group=group, topic=topic, offsets=offsets, records=1,
        dlq_records=0,
    )  # fmt: skip
    return op, props


def test_walk_filters_group_topic_and_operation() -> None:
    summaries = [  # newest first
        snap("append", "g", "t", {0: 30, 1: 12}),
        snap("append", "other", "t", {0: 999}),
        snap("append", "g", "other-topic", {0: 999}),
        snap("replace", "g", "t", {0: 999}),
        snap("append", "g", "t", {0: 20, 2: 7}),
        ("append", {}),  # a snapshot from another writer
    ]
    assert walk_last_offsets(summaries, "g", "t") == {0: 30, 1: 12, 2: 7}
    assert walk_last_offsets([], "g", "t") == {}


@given(
    st.lists(
        st.dictionaries(st.integers(0, 3), st.integers(0, 1000), max_size=4),
        max_size=10,
    )
)
def test_walk_is_per_partition_max(batches: list[dict[int, int]]) -> None:
    expected: dict[int, int] = {}
    for b in batches:
        for p, o in b.items():
            expected[p] = max(expected.get(p, o), o)
    got = walk_last_offsets([snap("append", "g", "t", b) for b in batches], "g", "t")
    assert got == expected


def test_find_batch() -> None:
    summaries = [snap("append", "g", "t", {0: 1}, run="r1", batch=4), snap("replace", "g", "t", {})]
    assert find_batch(summaries, "r1", 4)
    assert not find_batch(summaries, "r1", 5)
    assert not find_batch(summaries, "r2", 4)


class FakeTable:
    """Just enough of pyiceberg Table for append_batch."""

    def __init__(self, failures: list[Exception], commit_before_failing: bool = False) -> None:
        self.failures = failures
        self.commit_before_failing = commit_before_failing
        self.snapshots: list[Any] = []
        self.appended: list[int] = []
        self.refreshes = 0
        self.metadata = SimpleNamespace(snapshots=self.snapshots)
        self._schema = bronze_schema(load_schema(1))

    def refresh(self) -> "FakeTable":
        self.refreshes += 1
        return self

    def schema(self) -> Any:
        return self._schema

    def append(self, arrow: pa.Table, snapshot_properties: dict[str, str]) -> None:
        def commit() -> None:
            self.appended.append(arrow.num_rows)
            summary = SimpleNamespace(
                operation=SimpleNamespace(value="append"),
                additional_properties=snapshot_properties,
            )
            self.snapshots.append(SimpleNamespace(summary=summary))

        if self.failures:
            if self.commit_before_failing:
                commit()
            raise self.failures.pop(0)
        commit()


def _rows(n: int) -> list[dict[str, Any]]:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        build_row(Rec("t", 0, i, 1, b"k", b""), 1, {}, run_id="r", batch_id=1, ingested_at=now)
        for i in range(n)
    ]


def _summary() -> dict[str, str]:
    return build_summary(
        run_id="r", batch_id=1, group="g", topic="t", offsets={0: 2}, records=3, dlq_records=0
    )


def test_append_retries_a_commit_conflict_without_sleeping() -> None:
    table = FakeTable([CommitFailedException("stale")])
    sleeps: list[float] = []
    append_batch(table, _rows(3), _summary(), first_offsets={0: 0}, retries=5, sleep=sleeps.append)  # type: ignore[arg-type]
    assert table.appended == [3] and sleeps == []


def test_append_with_unknown_outcome_does_not_write_twice() -> None:
    table = FakeTable([ConnectionError("lost response")], commit_before_failing=True)
    sleeps: list[float] = []
    append_batch(table, _rows(3), _summary(), first_offsets={0: 0}, retries=5, sleep=sleeps.append)  # type: ignore[arg-type]
    assert table.appended == [3] and sleeps == [1]


def test_append_backs_off_then_gives_up() -> None:
    table = FakeTable([ConnectionError("down")] * 10)
    sleeps: list[float] = []
    with pytest.raises(ConnectionError):
        append_batch(
            table, _rows(1), _summary(), first_offsets={0: 0}, retries=5, sleep=sleeps.append
        )  # type: ignore[arg-type]
    assert sleeps == [1, 2, 4, 8, 16] and table.appended == []


_OFFSETS = st.dictionaries(st.integers(0, 5), st.integers(0, 100), max_size=6)


@given(written=_OFFSETS, first=_OFFSETS)
def test_overlapping_partitions(written: dict[int, int], first: dict[int, int]) -> None:
    got = overlapping_partitions(written, first)
    for p, w in written.items():
        assert (p in got) == (p in first and w >= first[p])
        assert got.get(p, w) == w


def test_append_fenced_by_a_zombie_write_is_never_retried() -> None:
    table = FakeTable([])
    append_batch(table, _rows(3), _summary(), first_offsets={0: 0}, retries=5)  # type: ignore[arg-type]
    sleeps: list[float] = []
    with pytest.raises(OffsetFenceError, match="partition 0 written up to 2, batch starts at 2"):
        append_batch(
            table,  # type: ignore[arg-type]
            _rows(3),
            _summary(),
            first_offsets={0: 2},
            retries=5,
            sleep=sleeps.append,
        )
    assert table.appended == [3] and sleeps == []


def test_summary_offsets_are_json() -> None:
    assert json.loads(_summary()["ingest.offsets"]) == {"0": 2}

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from lakestream.spark.checkpoint import clamp_older_than, committed_snapshot_id, parse_offset_file

# The shape Spark 4.1 writes with an Iceberg 1.12 source (one source per query).
OFFSET_FILE = (
    "v1\n"
    '{"batchWatermarkMs":0,"batchTimestampMs":1767268800000,'
    '"conf":{"spark.sql.shuffle.partitions":"2"}}\n'
    '{"version":1,"snapshot_id":"6127893840265894521","position":3,"scan_all_files":false}'
)


def test_parse_offset_file() -> None:
    assert parse_offset_file(OFFSET_FILE) == 6127893840265894521


def test_parse_accepts_a_numeric_id_and_skips_sources_without_offset() -> None:
    text = (
        'v1\n{"batchWatermarkMs":0}\n-\n'
        '{"version":1,"snapshot_id":42,"position":0,"scan_all_files":false}\n'
    )
    assert parse_offset_file(text) == 42


def test_parse_returns_none_without_a_snapshot() -> None:
    assert parse_offset_file("v1\n{}\n-") is None
    assert parse_offset_file("") is None
    assert parse_offset_file("v1\n{}\nnot json") is None
    # The snapshot id is never read from the metadata line.
    assert parse_offset_file('v1\n{"snapshot_id":7}\n-') is None


def test_parse_rejects_an_unknown_log_version() -> None:
    with pytest.raises(ValueError, match="offset log version"):
        parse_offset_file(OFFSET_FILE.replace("v1", "v2", 1))


def _write(checkpoint: Path, log: str, batch: int, text: str) -> None:
    (checkpoint / log).mkdir(parents=True, exist_ok=True)
    (checkpoint / log / str(batch)).write_text(text, encoding="utf-8")


def _offsets(snap: int) -> str:
    return f'v1\n{{}}\n{{"version":1,"snapshot_id":"{snap}","position":0}}'


def test_committed_snapshot_uses_the_last_committed_batch(tmp_path: Path) -> None:
    for batch in (2, 9, 10, 11):
        _write(tmp_path, "offsets", batch, _offsets(batch * 10))
    for batch in (2, 9, 10):  # batch 11 started (offsets/11) but never committed
        _write(tmp_path, "commits", batch, 'v1\n{"nextBatchWatermarkMs":0}')
    (tmp_path / "commits" / ".10.crc").write_text("junk", encoding="utf-8")
    assert committed_snapshot_id(tmp_path) == 100


def test_committed_snapshot_without_a_commit(tmp_path: Path) -> None:
    assert committed_snapshot_id(tmp_path) is None
    _write(tmp_path, "offsets", 0, _offsets(5))
    assert committed_snapshot_id(tmp_path) is None  # batch 0 planned, not committed
    _write(tmp_path, "commits", 1, "v1\n{}")
    assert committed_snapshot_id(tmp_path) is None  # offsets/1 already purged or missing


def test_clamp_older_than() -> None:
    t = datetime(2026, 1, 1, 12, tzinfo=UTC)
    early, late = t - timedelta(hours=1), t + timedelta(hours=1)
    assert clamp_older_than(t, [late, late]) == t  # silver is past the cutoff
    assert clamp_older_than(t, [late, early]) == early  # the slowest query wins
    assert clamp_older_than(t, [early]) == early
    assert clamp_older_than(t, [late, None]) is None  # unknown position: no expiry
    assert clamp_older_than(t, []) is None

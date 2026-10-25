from pathlib import Path

from lakestream.spark.checkpoint import latest_snapshot_id, parse_offset_file

OFFSET_FILE = (
    "v1\n"
    '{"batchWatermarkMs":0,"batchTimestampMs":1767268800000,'
    '"conf":{"spark.sql.shuffle.partitions":"2"}}\n'
    '{"version":1,"snapshot_id":"6127893840265894521","position":3,"scan_all_files":false}'
)


def test_parse_offset_file() -> None:
    assert parse_offset_file(OFFSET_FILE) == 6127893840265894521


def test_parse_accepts_a_numeric_id_and_skips_sources_without_offset() -> None:
    text = 'v1\n{"batchWatermarkMs":0}\n-\n{"version":1,"snapshot_id":42}\n'
    assert parse_offset_file(text) == 42


def test_parse_returns_none_without_a_snapshot() -> None:
    assert parse_offset_file("v1\n{}\n-") is None
    assert parse_offset_file("") is None
    assert parse_offset_file("v1\n{}\nnot json") is None


def test_latest_snapshot_id_uses_the_highest_batch(tmp_path: Path) -> None:
    offsets = tmp_path / "offsets"
    offsets.mkdir()
    for batch, snap in (("2", 20), ("10", 100), ("9", 90)):
        (offsets / batch).write_text(f'v1\n{{}}\n{{"snapshot_id":"{snap}"}}', encoding="utf-8")
    (offsets / ".10.crc").write_text("junk", encoding="utf-8")
    assert latest_snapshot_id(tmp_path) == 100


def test_latest_snapshot_id_without_offsets(tmp_path: Path) -> None:
    assert latest_snapshot_id(tmp_path) is None
    (tmp_path / "offsets").mkdir()
    assert latest_snapshot_id(tmp_path) is None

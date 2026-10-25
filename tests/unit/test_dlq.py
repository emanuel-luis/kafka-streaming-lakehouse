from datetime import UTC, datetime

from lakestream.consumer.batch import Rec
from lakestream.consumer.decode import AVRO_DECODE, DecodeFailure
from lakestream.consumer.dlq import build_headers
from lakestream.ops.dlq_show import DlqRecord, format_record, hex_preview


def test_headers_match_the_contract() -> None:
    rec = Rec("shop.events", 3, 42, 1_767_225_600_123, b"u1", b"\x00bad")
    headers = build_headers(
        rec,
        DecodeFailure(AVRO_DECODE, "EOFError: x"),
        group="bronze-ingest",
        run_id="r1",
        failed_at=datetime(2026, 1, 1, 0, 0, 1, 500_000, UTC),
    )
    assert dict(headers) == {
        "dlq.error.class": b"AvroDecodeError",
        "dlq.error.message": b"EOFError: x",
        "dlq.original.topic": b"shop.events",
        "dlq.original.partition": b"3",
        "dlq.original.offset": b"42",
        "dlq.original.timestamp": b"1767225600123",
        "dlq.consumer.group": b"bronze-ingest",
        "dlq.run_id": b"r1",
        "dlq.failed_at": b"2026-01-01T00:00:01.500Z",
    }


def test_dlq_show_formats_headers_and_a_hex_preview() -> None:
    headers = [
        ("dlq.error.class", b"NotConfluentFramed"),
        ("dlq.original.offset", b"7"),
        ("x", None),
    ]
    rec = DlqRecord(0, 3, b"user-00001", b"not-avro:" + b"a" * 40, headers)
    assert format_record(rec).splitlines() == [
        "partition 0 offset 3 key user-00001",
        "  dlq.error.class: NotConfluentFramed",
        "  dlq.original.offset: 7",
        "  x: null",
        "  value: " + (b"not-avro:" + b"a" * 23).hex(" ") + " ... (49 bytes)",
    ]
    assert hex_preview(b"\x00\x01") == "00 01 (2 bytes)"
    assert hex_preview(None) == "null"
    assert "key null" in format_record(DlqRecord(0, 0, None, None, []))

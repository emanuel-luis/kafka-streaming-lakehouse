from datetime import UTC, datetime

from lakestream.consumer.batch import Rec
from lakestream.consumer.decode import AVRO_DECODE, DecodeFailure
from lakestream.consumer.dlq import build_headers


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

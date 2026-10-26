import random
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from frames import BRONZE_SCHEMA, T0, bronze_df
from pyspark.sql import SparkSession

from lakestream import contracts
from lakestream.producer.generator import GenParams, make_invalid
from lakestream.spark import silver

pytestmark = pytest.mark.spark


def _base(event_type: str, **over: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": T0,
        "user_id": "u1",
        "session_id": "s1",
    }
    extra: dict[str, dict[str, Any]] = {
        "page_view": {"page_url": "/shop/home"},
        "add_to_cart": {"product_id": "p1", "quantity": 2, "unit_price": Decimal("9.99")},
        "order_placed": {"order_id": "o1", "amount": Decimal("10.00"), "currency": "BRL"},
        "payment": {
            "order_id": "o1",
            "amount": Decimal("10.00"),
            "currency": "BRL",
            "payment_method": "card",
            "payment_status": "approved",
        },
    }
    return {**fields, **extra.get(event_type, {}), **over}


def _cases() -> list[dict[str, Any]]:
    cases = [_base(t) for t in contracts.EVENT_TYPES]
    for seed in range(6):
        for reason in contracts.REJECT_REASONS:
            event = make_invalid(random.Random(seed), GenParams(), reason)
            cases.append(event.as_record())
    cases += [
        _base("page_view", event_id=str(uuid.uuid4()).upper()),
        _base("page_view", event_id=str(uuid.uuid4()) + "\n"),
        _base("page_view", event_id=None),
        {**_base("page_view"), "event_type": None},
        _base("page_view", user_id=""),
        _base("page_view", session_id=None),
        _base("add_to_cart", quantity=1),
        _base("add_to_cart", quantity=100),
        _base("add_to_cart", quantity=101),
        _base("add_to_cart", unit_price=Decimal("-1.00")),
        _base("add_to_cart", quantity=0, unit_price=None),
        _base("order_placed", currency="BRLX"),
        _base("order_placed", currency="BR"),
        _base("payment", payment_status="declined"),
        _base("payment", payment_method=None),
        _base("payment", amount=Decimal("0.01")),
    ]
    return cases


def test_validate_mirrors_the_reference(spark: SparkSession) -> None:
    cases = _cases()
    rows = silver.validate(bronze_df(spark, cases)).orderBy("kafka_offset").collect()
    got = [r["reject_reason"] for r in rows]
    assert got == [contracts.validate(c) for c in cases]
    assert set(contracts.REJECT_REASONS) <= set(got)
    assert None in got


def test_validate_adds_only_reject_reason(spark: SparkSession) -> None:
    df = silver.validate(bronze_df(spark, [_base("page_view")]))
    assert df.columns == [*BRONZE_SCHEMA.fieldNames(), "reject_reason"]
    assert df.schema["reject_reason"].dataType.simpleString() == "string"


def test_dedupe_on_a_batch(spark: SparkSession) -> None:
    first, other = _base("page_view"), _base("page_view")
    df = bronze_df(spark, [first, first, other])
    ids = sorted(r.event_id for r in silver.dedupe(df).collect())
    assert ids == sorted([first["event_id"], other["event_id"]])


def test_dedupe_on_a_stream_across_two_batches(spark: SparkSession, tmp_path: Path) -> None:
    src, out, ckpt = tmp_path / "in", tmp_path / "out", tmp_path / "ckpt"
    dup, a, b = _base("page_view"), _base("page_view"), _base("page_view")

    def run() -> None:
        stream = spark.readStream.schema(BRONZE_SCHEMA).parquet(str(src / "*"))
        query = (
            silver.dedupe(stream)
            .writeStream.format("parquet")
            .option("path", str(out))
            .option("checkpointLocation", str(ckpt))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

    bronze_df(spark, [dup, a]).write.parquet(str(src / "b1"))
    run()
    bronze_df(spark, [dup, b]).write.parquet(str(src / "b2"))
    run()
    ids = sorted(r.event_id for r in spark.read.parquet(str(out)).collect())
    assert ids == sorted([dup["event_id"], a["event_id"], b["event_id"]])


def test_projections_and_split(spark: SparkSession) -> None:
    good = _base("payment")
    bad = _base("order_placed", currency="usd")
    df = bronze_df(spark, [good, bad])
    events = silver.silver_events(df)
    rejects = silver.silver_rejects(df)
    assert events.columns == [*silver.SILVER_COLUMNS, "processed_at"]
    assert rejects.columns == [
        "event_id",
        "event_type",
        "event_time",
        "user_id",
        "reject_reason",
        "record_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
        "processed_at",
    ]
    [e] = events.collect()
    assert (e.event_id, e.amount, e.payment_status) == (
        good["event_id"],
        Decimal("10.00"),
        "approved",
    )
    [r] = rejects.collect()
    assert (r.event_id, r.reject_reason, r.kafka_offset) == (bad["event_id"], "invalid_currency", 1)
    assert '"currency":"usd"' in r.record_json
    assert '"kafka_topic":"shop.events"' in r.record_json
    assert "reject_reason" not in r.record_json


def test_silver_events_dedupes_valid_rows(spark: SparkSession) -> None:
    ev = _base("page_view")
    later = {**ev, "event_time": T0 + timedelta(minutes=1)}
    assert len(silver.silver_events(bronze_df(spark, [ev, later])).collect()) == 1

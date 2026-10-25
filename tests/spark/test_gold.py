from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakestream.spark import gold

pytestmark = pytest.mark.spark

SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("user_id", StringType()),
        StructField("amount", DecimalType(12, 2)),
        StructField("currency", StringType()),
        StructField("payment_status", StringType()),
    ]
)


def at(clock: str) -> datetime:
    """Naive 2026-01-01 at "H:M" or "H:M:S[.fff]"; collected rows are UTC (see conftest)."""
    h, m, *rest = clock.split(":")
    seconds = float(rest[0]) if rest else 0.0
    whole = int(seconds)
    micros = round((seconds - whole) * 1_000_000)
    return datetime(2026, 1, 1, int(h), int(m), whole, micros)


def silver(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    data = [(t, at(c), u, a, cur, st) for t, c, u, a, cur, st in rows]
    return spark.createDataFrame(data, SCHEMA)


def dec(x: str) -> Decimal:
    return Decimal(x)


def test_events_per_minute(spark: SparkSession) -> None:
    df = silver(
        spark,
        [
            ("page_view", "12:00:10", "u1", None, None, None),
            ("page_view", "12:00:50", "u1", None, None, None),
            ("page_view", "12:00:59", "u2", None, None, None),
            ("add_to_cart", "12:01:00", "u1", None, None, None),
        ],
    )
    out = gold.events_per_minute(df)
    assert out.columns == [
        "window_start",
        "window_end",
        "event_type",
        "event_count",
        "unique_users",
    ]
    got = [tuple(r) for r in out.orderBy("window_start").collect()]
    assert got == [
        (at("12:00"), at("12:01"), "page_view", 3, 2),
        (at("12:01"), at("12:02"), "add_to_cart", 1, 1),
    ]


def test_revenue_5min(spark: SparkSession) -> None:
    df = silver(
        spark,
        [
            ("order_placed", "12:01", "u1", dec("100.00"), "BRL", None),
            ("payment", "12:02", "u1", dec("100.00"), "BRL", "approved"),
            ("payment", "12:03", "u2", dec("50.50"), "BRL", "declined"),
            ("order_placed", "12:04:59", "u3", dec("20.25"), "BRL", None),
            ("order_placed", "12:05", "u4", dec("10.00"), "USD", None),
            ("page_view", "12:01", "u1", None, None, None),
        ],
    )
    out = gold.revenue_5min(df)
    assert out.columns == [
        "window_start",
        "window_end",
        "currency",
        "orders_placed",
        "order_amount",
        "payments_approved",
        "payments_declined",
        "approved_amount",
    ]
    got = [tuple(r) for r in out.orderBy("window_start", "currency").collect()]
    assert got == [
        (at("12:00"), at("12:05"), "BRL", 2, dec("120.25"), 1, 1, dec("100.00")),
        (at("12:05"), at("12:10"), "USD", 1, dec("10.00"), 0, 0, dec("0.00")),
    ]


def test_user_sessions(spark: SparkSession) -> None:
    df = silver(
        spark,
        [
            ("page_view", "12:00", "u1", None, None, None),
            ("add_to_cart", "12:10", "u1", None, None, None),
            ("order_placed", "12:20:30", "u1", dec("120.00"), "BRL", None),
            ("payment", "12:25:00.750", "u1", dec("120.00"), "BRL", "approved"),
            ("page_view", "13:00", "u1", None, None, None),
            ("payment", "12:00", "u2", dec("5.00"), "USD", "declined"),
        ],
    )
    out = gold.user_sessions(df)
    assert out.columns == [
        "user_id",
        "session_start",
        "last_event_at",
        "session_window_end",
        "duration_seconds",
        "event_count",
        "page_views",
        "add_to_carts",
        "orders_placed",
        "payments_approved",
        "revenue",
        "reached_cart",
        "reached_order",
        "reached_payment",
    ]
    got = [tuple(r) for r in out.orderBy("user_id", "session_start").collect()]
    z = dec("0.00")
    assert got == [
        ("u1", at("12:00"), at("12:25:00.750"), at("12:55:00.750"), 1500, 4, 1, 1, 1, 1)
        + (dec("120.00"), True, True, True),
        ("u1", at("13:00"), at("13:00"), at("13:30"), 0, 1, 1, 0, 0, 0) + (z, False, False, False),
        ("u2", at("12:00"), at("12:00"), at("12:30"), 0, 1, 0, 0, 0, 0) + (z, False, False, True),
    ]

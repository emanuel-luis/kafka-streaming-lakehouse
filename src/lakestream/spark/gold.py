"""Gold transforms: batch recompute from silver events. Pure DataFrame in, DataFrame out."""

from decimal import Decimal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _count_if(cond: Column) -> Column:
    return F.count(F.when(cond, 1))


def _sum_if(cond: Column) -> Column:
    return F.coalesce(F.sum(F.when(cond, F.col("amount"))), F.lit(Decimal("0.00")))


def _is(event_type: str) -> Column:
    return F.col("event_type") == event_type


def _approved() -> Column:
    return _is("payment") & (F.col("payment_status") == "approved")


def events_per_minute(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy(F.window("event_time", "1 minute").alias("w"), "event_type")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "event_type",
            "event_count",
            "unique_users",
        )
    )


def revenue_5min(silver: DataFrame) -> DataFrame:
    return (
        silver.where(F.col("event_type").isin("order_placed", "payment"))
        .groupBy(F.window("event_time", "5 minutes").alias("w"), "currency")
        .agg(
            _count_if(_is("order_placed")).alias("orders_placed"),
            _sum_if(_is("order_placed")).alias("order_amount"),
            _count_if(_approved()).alias("payments_approved"),
            _count_if(_is("payment") & (F.col("payment_status") == "declined")).alias(
                "payments_declined"
            ),
            _sum_if(_approved()).alias("approved_amount"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "currency",
            "orders_placed",
            "order_amount",
            "payments_approved",
            "payments_declined",
            "approved_amount",
        )
    )


def user_sessions(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("user_id", F.session_window("event_time", "30 minutes").alias("w"))
        .agg(
            F.max("event_time").alias("last_event_at"),
            F.count(F.lit(1)).alias("event_count"),
            _count_if(_is("page_view")).alias("page_views"),
            _count_if(_is("add_to_cart")).alias("add_to_carts"),
            _count_if(_is("order_placed")).alias("orders_placed"),
            _count_if(_approved()).alias("payments_approved"),
            _sum_if(_approved()).alias("revenue"),
            F.bool_or(_is("add_to_cart")).alias("reached_cart"),
            F.bool_or(_is("order_placed")).alias("reached_order"),
            F.bool_or(_is("payment")).alias("reached_payment"),
        )
        .select(
            "user_id",
            F.col("w.start").alias("session_start"),
            "last_event_at",
            F.col("w.end").alias("session_window_end"),
            ((F.unix_micros("last_event_at") - F.unix_micros(F.col("w.start"))) / 1_000_000)
            .cast("long")
            .alias("duration_seconds"),
            "event_count",
            "page_views",
            "add_to_carts",
            "orders_placed",
            "payments_approved",
            "revenue",
            "reached_cart",
            "reached_order",
            "reached_payment",
        )
    )

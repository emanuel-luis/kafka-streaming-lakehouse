"""Small DataFrame builders shared by the Spark tests."""

from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("page_url", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("order_id", StringType()),
        StructField("amount", DecimalType(12, 2)),
        StructField("currency", StringType()),
        StructField("payment_method", StringType()),
        StructField("payment_status", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("kafka_key", StringType()),
        StructField("schema_id", IntegerType()),
        StructField("ingested_at", TimestampType()),
        StructField("run_id", StringType()),
        StructField("batch_id", LongType()),
    ]
)

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def bronze_row(offset: int, record: dict[str, Any]) -> dict[str, Any]:
    """An event record plus the Kafka and ingest columns; offset is its unique coordinate."""
    return {
        **record,
        "kafka_topic": "shop.events",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp": T0,
        "kafka_key": record.get("user_id"),
        "schema_id": 1,
        "ingested_at": T0,
        "run_id": "r",
        "batch_id": 1,
    }


def bronze_df(spark: SparkSession, records: list[dict[str, Any]]) -> DataFrame:
    rows = [
        tuple(bronze_row(i, r).get(f.name) for f in BRONZE_SCHEMA.fields)
        for i, r in enumerate(records)
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)

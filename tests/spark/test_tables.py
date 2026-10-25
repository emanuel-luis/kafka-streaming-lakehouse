from collections.abc import Callable

import pytest
from frames import bronze_df
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from lakestream.spark import gold, silver, tables

pytestmark = pytest.mark.spark


def _shape(schema: StructType) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in schema.fields]


def test_silver_outputs_match_their_tables(spark: SparkSession) -> None:
    bronze = bronze_df(spark, [])
    assert _shape(silver.silver_events(bronze).schema) == _shape(tables.SILVER_EVENTS)
    assert _shape(silver.silver_rejects(bronze).schema) == _shape(tables.SILVER_REJECTS)


@pytest.mark.parametrize(
    ("transform", "schema"),
    [
        (gold.events_per_minute, tables.EVENTS_PER_MINUTE),
        (gold.revenue_5min, tables.REVENUE_5MIN),
        (gold.user_sessions, tables.USER_SESSIONS),
    ],
)
def test_gold_outputs_match_their_tables(
    spark: SparkSession, transform: Callable[[DataFrame], DataFrame], schema: StructType
) -> None:
    silver_events = spark.createDataFrame([], tables.SILVER_EVENTS)
    assert _shape(transform(silver_events).schema) == _shape(schema)


def test_create_table_sql() -> None:
    sql = tables.create_table_sql(tables.TABLES[2])
    assert sql == (
        "CREATE TABLE IF NOT EXISTS gold.events_per_minute (`window_start` timestamp, "
        "`window_end` timestamp, `event_type` string, `event_count` bigint, "
        "`unique_users` bigint) USING iceberg PARTITIONED BY (days(window_start)) "
        "TBLPROPERTIES ('format-version'='2')"
    )

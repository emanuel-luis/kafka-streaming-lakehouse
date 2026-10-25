import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

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


def test_add_columns_sql_places_each_missing_column() -> None:
    events = tables.TABLES[0]
    names = events.schema.fieldNames()
    assert tables.add_columns_sql(events, names) == []
    without = [n for n in names if n != "device_type"]
    assert tables.add_columns_sql(events, without) == [
        "ALTER TABLE silver.events ADD COLUMN `device_type` string AFTER `payment_status`"
    ]
    assert tables.add_columns_sql(events, names[1:])[0].endswith("`event_id` string FIRST")


def test_ensure_tables_adds_a_missing_column_on_iceberg(tmp_path: Path) -> None:
    """An older silver.events without device_type gains it in contract order; a rerun is a no-op.

    Runs in a child process with the Iceberg runtime (resolved through Ivy, so the first run needs
    Maven Central or a warm `SPARK_IVY_DIR`) and a Hadoop catalog under tmp_path.
    """
    script = Path(__file__).with_name("ensure_tables_iceberg.py")
    result = subprocess.run(
        [sys.executable, str(script), (tmp_path / "warehouse").as_uri()],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    assert "ensure_tables ok" in result.stdout

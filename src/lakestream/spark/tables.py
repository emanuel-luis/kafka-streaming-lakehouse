"""Schemas and DDL of every table Spark writes. Each schema is defined once, here.

Types match the transform outputs exactly, because Iceberg never narrows a type on write: counts
are `bigint`, and `sum` over `decimal(12,2)` gives `decimal(22,2)`.
"""

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _schema(*fields: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, t) for name, t in fields])


_TS, _STR, _INT, _LONG, _BOOL = (
    TimestampType(),
    StringType(),
    IntegerType(),
    LongType(),
    BooleanType(),
)
_SUM = DecimalType(22, 2)

SILVER_EVENTS = _schema(
    ("event_id", _STR),
    ("event_type", _STR),
    ("event_time", _TS),
    ("user_id", _STR),
    ("session_id", _STR),
    ("page_url", _STR),
    ("product_id", _STR),
    ("quantity", _INT),
    ("unit_price", DecimalType(10, 2)),
    ("order_id", _STR),
    ("amount", DecimalType(12, 2)),
    ("currency", _STR),
    ("payment_method", _STR),
    ("payment_status", _STR),
    ("kafka_partition", _INT),
    ("kafka_offset", _LONG),
    ("ingested_at", _TS),
    ("processed_at", _TS),
)
SILVER_REJECTS = _schema(
    ("event_id", _STR),
    ("event_type", _STR),
    ("event_time", _TS),
    ("user_id", _STR),
    ("reject_reason", _STR),
    ("record_json", _STR),
    ("kafka_topic", _STR),
    ("kafka_partition", _INT),
    ("kafka_offset", _LONG),
    ("ingested_at", _TS),
    ("processed_at", _TS),
)
EVENTS_PER_MINUTE = _schema(
    ("window_start", _TS),
    ("window_end", _TS),
    ("event_type", _STR),
    ("event_count", _LONG),
    ("unique_users", _LONG),
)
REVENUE_5MIN = _schema(
    ("window_start", _TS),
    ("window_end", _TS),
    ("currency", _STR),
    ("orders_placed", _LONG),
    ("order_amount", _SUM),
    ("payments_approved", _LONG),
    ("payments_declined", _LONG),
    ("approved_amount", _SUM),
)
USER_SESSIONS = _schema(
    ("user_id", _STR),
    ("session_start", _TS),
    ("last_event_at", _TS),
    ("session_window_end", _TS),
    ("duration_seconds", _LONG),
    ("event_count", _LONG),
    ("page_views", _LONG),
    ("add_to_carts", _LONG),
    ("orders_placed", _LONG),
    ("payments_approved", _LONG),
    ("revenue", _SUM),
    ("reached_cart", _BOOL),
    ("reached_order", _BOOL),
    ("reached_payment", _BOOL),
)
JOB_RUNS = _schema(
    ("run_id", _STR),
    ("job", _STR),
    ("started_at", _TS),
    ("finished_at", _TS),
    ("status", _STR),
    ("rows_in", _LONG),
    ("rows_out", _LONG),
    ("rows_rejected", _LONG),
    ("rows_dropped_late", _LONG),
    ("details", _STR),
)


@dataclass(frozen=True)
class TableDef:
    name: str
    schema: StructType
    partition: str | None


TABLES = (
    TableDef("silver.events", SILVER_EVENTS, "days(event_time)"),
    TableDef("silver.rejects", SILVER_REJECTS, "days(processed_at)"),
    TableDef("gold.events_per_minute", EVENTS_PER_MINUTE, "days(window_start)"),
    TableDef("gold.revenue_5min", REVENUE_5MIN, "days(window_start)"),
    TableDef("gold.user_sessions", USER_SESSIONS, "days(session_start)"),
    TableDef("ops.job_runs", JOB_RUNS, None),  # a few rows per run: no partitioning
)


def create_table_sql(table: TableDef) -> str:
    columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in table.schema.fields)
    partition = f" PARTITIONED BY ({table.partition})" if table.partition else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {table.name} ({columns}) USING iceberg{partition}"
        " TBLPROPERTIES ('format-version'='2')"
    )


def ensure_tables(spark: SparkSession) -> None:
    """Create the namespaces and tables when missing (in the session's default catalog)."""
    for namespace in sorted({t.name.split(".")[0] for t in TABLES}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    for table in TABLES:
        spark.sql(create_table_sql(table))

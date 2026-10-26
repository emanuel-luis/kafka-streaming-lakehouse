"""Run by test_tables.py in its own process: ensure_tables against a real Iceberg catalog.

The shared test session has no packages, and a JVM gets its packages only at launch, so this
script starts a separate session with the Iceberg runtime and a Hadoop catalog on a local
warehouse. Usage: python ensure_tables_iceberg.py WAREHOUSE_DIR
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from lakestream.spark import tables
from lakestream.spark.session import PACKAGES


def main(warehouse: str) -> None:
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("lakestream-ensure-tables-test")
        .config("spark.jars.packages", PACKAGES[0])  # the runtime; no S3 here
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", "lake")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if os.environ.get("SPARK_IVY_DIR"):
        builder = builder.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
    spark = builder.getOrCreate()
    try:
        events = tables.TABLES[0]
        assert events.name == "silver.events"
        contract = events.schema.fieldNames()
        old = tables.TableDef(
            events.name,
            StructType([f for f in events.schema.fields if f.name != "device_type"]),
            events.partition,
        )
        spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
        spark.sql(tables.create_table_sql(old))
        assert "device_type" not in spark.table(events.name).columns

        def metadata_versions() -> int:
            return spark.sql(f"SELECT * FROM {events.name}.metadata_log_entries").count()

        tables.ensure_tables(spark)
        assert spark.table(events.name).columns == contract, spark.table(events.name).columns
        after_first = metadata_versions()
        tables.ensure_tables(spark)
        assert spark.table(events.name).columns == contract
        assert metadata_versions() == after_first, "the second ensure_tables changed the table"
        print("ensure_tables ok")
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])

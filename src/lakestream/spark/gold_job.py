"""`lakestream gold`: recompute every gold table from silver, one snapshot per table."""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakestream.spark import gold
from lakestream.spark.runlog import JobRun, write_run
from lakestream.spark.tables import ensure_tables

log = logging.getLogger("lakestream.gold")

GOLD = (
    ("gold.events_per_minute", gold.events_per_minute),
    ("gold.revenue_5min", gold.revenue_5min),
    ("gold.user_sessions", gold.user_sessions),
)


def run_gold(spark: SparkSession, *, run_id: str) -> None:
    ensure_tables(spark)
    run = JobRun(run_id, "gold")
    # Pin the current snapshot: the three tables come from the same rows even if silver commits
    # meanwhile. An empty table has no `main` ref yet.
    ref = spark.sql("SELECT snapshot_id FROM silver.events.refs WHERE name = 'main'").first()
    reader = spark.read
    if ref is not None:
        reader = reader.option("snapshot-id", str(ref["snapshot_id"]))
        run.details["silver_snapshot_id"] = ref["snapshot_id"]
    silver = reader.table("silver.events")
    try:
        run.rows_in = silver.count()
        for table, transform in GOLD:
            (
                transform(silver)
                .writeTo(table)
                .option("snapshot-property.run_id", run_id)
                .overwrite(F.lit(True))
            )
            written = spark.table(table).count()
            run.details[table] = written
            run.rows_out += written
            log.info("gold table replaced", extra={"table": table, "rows": written})
        run.status = "succeeded"
    except BaseException:
        run.status = "failed"
        raise
    finally:
        write_run(spark, run)

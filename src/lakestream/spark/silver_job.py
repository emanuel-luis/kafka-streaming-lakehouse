"""`lakestream silver`: two streaming queries from bronze into silver events and rejects."""

import logging
import signal
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery

from lakestream.spark import silver
from lakestream.spark.runlog import JobRun, write_run
from lakestream.spark.tables import ensure_tables

log = logging.getLogger("lakestream.silver")

BRONZE = "bronze.events"
TRIGGERS = ("available-now", "processing-time")
# (query name = checkpoint dir, target table, transform)
QUERIES = (
    ("silver_events", "silver.events", silver.silver_events),
    ("silver_rejects", "silver.rejects", silver.silver_rejects),
)


def read_bronze(spark: SparkSession) -> DataFrame:
    """Bronze as a stream; replace snapshots (compaction) are skipped by the source itself."""
    return (
        spark.readStream.format("iceberg")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .load(BRONZE)
    )


def progress_totals(progress: Iterable[Any], name: str) -> dict[str, int]:
    """Sum one query's `recentProgress`: input rows, rows written (observed) and late drops."""
    totals = {"batches": 0, "rows_in": 0, "rows_out": 0, "rows_dropped_late": 0}
    for p in progress:
        totals["batches"] += 1
        totals["rows_in"] += p.numInputRows
        observed = p.observedMetrics.get(name)
        totals["rows_out"] += observed["rows"] if observed is not None else 0
        totals["rows_dropped_late"] += sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
    return totals


def start_query(
    df: DataFrame, name: str, table: str, checkpoint_root: Path, trigger: str, interval: str
) -> StreamingQuery:
    writer = (
        df.observe(name, F.count(F.lit(1)).alias("rows"))
        .writeStream.format("iceberg")
        .queryName(name)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_root / name))
        .option("fanout-enabled", "true")  # rows of several days may share one task
    )
    if trigger == "available-now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=interval)
    return writer.toTable(table)


def _wait_for_signal(queries: list[StreamingQuery]) -> None:
    """Run until SIGTERM or SIGINT; a query that dies raises its exception here."""
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.wait(5):
        for q in queries:
            if not q.isActive:
                q.awaitTermination()  # raises StreamingQueryException if it failed
                raise RuntimeError(f"query {q.name} stopped on its own")
    log.info("stop signal received, stopping the queries")


def run_silver(
    spark: SparkSession, *, run_id: str, checkpoint_root: Path, trigger: str, interval: str
) -> None:
    if trigger not in TRIGGERS:
        raise ValueError(f"trigger must be one of {TRIGGERS}")
    ensure_tables(spark)
    run = JobRun(run_id, "silver", details={"trigger": trigger})
    if trigger == "processing-time":
        run.details["interval"] = interval
    queries: list[StreamingQuery] = []
    try:
        for name, table, transform in QUERIES:
            queries.append(
                start_query(
                    transform(read_bronze(spark)), name, table, checkpoint_root, trigger, interval
                )
            )
            log.info("query started", extra={"query": name, "table": table})
        if trigger == "available-now":
            for q in queries:
                q.awaitTermination()
        else:
            _wait_for_signal(queries)
        run.status = "succeeded"
    except BaseException:
        run.status = "failed"
        raise
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        # ponytail: recentProgress keeps the last 100 batches (numRecentProgressUpdates);
        # a long processing-time run under-counts, a StreamingQueryListener would not.
        totals = {q.name: progress_totals(q.recentProgress, q.name) for q in queries}
        events = totals.get("silver_events", {})
        run.rows_in = events.get("rows_in", 0)
        run.rows_out = events.get("rows_out", 0)
        run.rows_rejected = totals.get("silver_rejects", {}).get("rows_out", 0)
        run.rows_dropped_late = events.get("rows_dropped_late", 0)
        run.details["queries"] = totals
        write_run(spark, run)
        log.info("silver run finished", extra={"status": run.status, **run.details})

"""`lakestream maintain`: compaction, manifest rewrite and snapshot expiry, table by table."""

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from lakestream.runmeta import utc_now
from lakestream.spark.checkpoint import clamp_older_than, committed_snapshot_id
from lakestream.spark.silver_job import BRONZE, QUERIES
from lakestream.spark.tables import TABLES

log = logging.getLogger("lakestream.maintain")

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ALL_TABLES = (BRONZE, *(t.name for t in TABLES))


def _commit_time(spark: SparkSession, snapshot_id: int) -> datetime | None:
    row = spark.sql(
        f"SELECT unix_millis(committed_at) AS ms FROM {BRONZE}.snapshots "
        f"WHERE snapshot_id = {int(snapshot_id)}"
    ).first()
    return None if row is None else EPOCH + timedelta(milliseconds=row["ms"])


def bronze_cutoff(
    spark: SparkSession, checkpoint_root: Path, requested: datetime
) -> datetime | None:
    commits: list[datetime | None] = []
    for name, _, _ in QUERIES:
        snapshot_id = committed_snapshot_id(checkpoint_root / name)
        commit = None if snapshot_id is None else _commit_time(spark, snapshot_id)
        if commit is None:
            log.warning(
                "not expiring bronze: no resume snapshot for a silver query",
                extra={"query": name, "snapshot_id": snapshot_id},
            )
        commits.append(commit)
    return clamp_older_than(requested, commits)


def _sql_ts(ts: datetime) -> str:
    """A TIMESTAMP literal in UTC (the session time zone is UTC)."""
    return f"TIMESTAMP '{ts.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f}'"


def run_maintain(
    spark: SparkSession,
    *,
    catalog: str,
    checkpoint_root: Path,
    older_than_minutes: int,
    retain_last: int,
    tables: list[str],
) -> None:
    requested = utc_now() - timedelta(minutes=older_than_minutes)
    for table in tables or ALL_TABLES:
        if not spark.catalog.tableExists(table):
            log.info("table missing, skipped", extra={"table": table})
            continue
        spark.sql(
            f"CALL {catalog}.system.rewrite_data_files(table => '{table}', "
            "options => map('min-input-files', '2'))"
        )
        spark.sql(f"CALL {catalog}.system.rewrite_manifests(table => '{table}')")
        cutoff = bronze_cutoff(spark, checkpoint_root, requested) if table == BRONZE else requested
        if cutoff is None:
            log.info("table compacted, expiry skipped", extra={"table": table})
            continue
        spark.sql(
            f"CALL {catalog}.system.expire_snapshots(table => '{table}', "
            f"older_than => {_sql_ts(cutoff)}, retain_last => {int(retain_last)})"
        )
        log.info("table maintained", extra={"table": table, "older_than": cutoff.isoformat()})

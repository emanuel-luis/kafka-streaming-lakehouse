"""`lakestream maintain`: compaction, manifest rewrite and snapshot expiry, table by table."""

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

from lakestream.runmeta import utc_now
from lakestream.spark.checkpoint import clamp_older_than, protected_snapshot_ids
from lakestream.spark.silver_job import BRONZE, QUERIES
from lakestream.spark.tables import TABLES

log = logging.getLogger("lakestream.maintain")

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ALL_TABLES = (BRONZE, *(t.name for t in TABLES))


def _commit_time(spark: SparkSession, catalog: str, snapshot_id: int) -> datetime | None:
    row = spark.sql(
        f"SELECT unix_millis(committed_at) AS ms FROM {catalog}.{BRONZE}.snapshots "
        f"WHERE snapshot_id = {int(snapshot_id)}"
    ).first()
    return None if row is None else EPOCH + timedelta(milliseconds=row["ms"])


def bronze_cutoff(
    spark: SparkSession, catalog: str, checkpoint_root: Path, requested: datetime
) -> datetime | None:
    """Never expire a bronze snapshot a silver query may still plan from (decisions 22 and 24)."""
    commits: list[datetime | None] = []
    for name, _, _ in QUERIES:
        snapshot_ids = protected_snapshot_ids(checkpoint_root / name)
        if snapshot_ids is None:
            log.warning("not expiring bronze: unknown silver checkpoint", extra={"query": name})
            return None
        for snapshot_id in snapshot_ids:
            commit = _commit_time(spark, catalog, snapshot_id)
            if commit is None:
                log.warning(
                    "not expiring bronze: a protected snapshot is gone",
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
        # Catalog-qualified: a bare `ns.table` makes Spark and the Iceberg procedures try `ns`
        # as a catalog first and log CATALOG_NOT_FOUND before falling back.
        qualified = f"{catalog}.{table}"
        if not spark.catalog.tableExists(qualified):
            log.info("table missing, skipped", extra={"table": table})
            continue
        spark.sql(
            f"CALL {catalog}.system.rewrite_data_files(table => '{qualified}', "
            "options => map('min-input-files', '2'))"
        )
        spark.sql(f"CALL {catalog}.system.rewrite_manifests(table => '{qualified}')")
        cutoff = (
            bronze_cutoff(spark, catalog, checkpoint_root, requested)
            if table == BRONZE
            else requested
        )
        if cutoff is None:
            log.info("table compacted, expiry skipped", extra={"table": table})
            continue
        spark.sql(
            f"CALL {catalog}.system.expire_snapshots(table => '{qualified}', "
            f"older_than => {_sql_ts(cutoff)}, retain_last => {int(retain_last)})"
        )
        log.info("table maintained", extra={"table": table, "older_than": cutoff.isoformat()})

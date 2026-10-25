"""One row per Spark job run in `ops.job_runs`."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from lakestream.runmeta import utc_now
from lakestream.spark.tables import JOB_RUNS


@dataclass
class JobRun:
    run_id: str
    job: str
    started_at: datetime = field(default_factory=utc_now)
    status: str = "running"
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    rows_dropped_late: int = 0
    details: dict[str, Any] = field(default_factory=dict)


def write_run(spark: SparkSession, run: JobRun) -> None:
    """Append the run with `finished_at` = now (UTC)."""
    row = (
        run.run_id,
        run.job,
        run.started_at,
        utc_now(),
        run.status,
        run.rows_in,
        run.rows_out,
        run.rows_rejected,
        run.rows_dropped_late,
        json.dumps(run.details, sort_keys=True, default=str),
    )
    spark.createDataFrame([row], JOB_RUNS).writeTo("ops.job_runs").append()

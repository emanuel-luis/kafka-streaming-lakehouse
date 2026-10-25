from pathlib import Path

import pytest
from frames import BRONZE_SCHEMA, bronze_df
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from test_silver import _base

from lakestream.spark import silver
from lakestream.spark.silver_job import progress_totals

pytestmark = pytest.mark.spark


def test_progress_totals_on_an_available_now_stream(spark: SparkSession, tmp_path: Path) -> None:
    dup, other, bad = _base("page_view"), _base("add_to_cart"), _base("page_view", user_id="")
    bronze_df(spark, [dup, dup, other, bad]).write.parquet(str(tmp_path / "in" / "b1"))
    stream = spark.readStream.schema(BRONZE_SCHEMA).parquet(str(tmp_path / "in" / "*"))
    query = (
        silver.silver_events(stream)
        .observe("silver_events", F.count(F.lit(1)).alias("rows"))
        .writeStream.format("parquet")
        .option("path", str(tmp_path / "out"))
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    totals = progress_totals(query.recentProgress, "silver_events")
    assert totals["rows_in"] == 4
    assert totals["rows_out"] == 2  # duplicate and invalid row dropped
    assert totals["rows_dropped_late"] == 0
    assert totals["batches"] >= 1

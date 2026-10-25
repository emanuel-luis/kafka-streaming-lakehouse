import os
import time
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

# Python collects timestamps as naive datetimes in the process time zone: pin it to UTC.
if hasattr(time, "tzset"):  # POSIX only; Spark tests never run on Windows
    os.environ["TZ"] = "UTC"
    time.tzset()


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakestream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

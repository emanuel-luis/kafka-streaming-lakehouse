"""The SparkSession of every job: Iceberg REST catalog `lake` over S3FileIO."""

import logging
from typing import TYPE_CHECKING

from lakestream.settings import IcebergSettings, S3Settings, SparkSettings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("lakestream.spark")

PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.12.0",
)


def session_conf(spark: SparkSettings, iceberg: IcebergSettings, s3: S3Settings) -> dict[str, str]:
    """Every Spark property the jobs set. Holds the S3 secret: never log the result."""
    cat = f"spark.sql.catalog.{iceberg.catalog}"
    conf = {
        "spark.master": spark.master,
        "spark.driver.memory": spark.driver_memory,
        "spark.jars.packages": ",".join(PACKAGES),
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.defaultCatalog": iceberg.catalog,
        # Required, not cosmetic: `to_json` in silver rejects renders timestamps in this zone.
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "4",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.type": "rest",
        f"{cat}.uri": iceberg.rest_uri,
        f"{cat}.warehouse": iceberg.warehouse,
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": s3.endpoint,
        f"{cat}.s3.path-style-access": "true",
        f"{cat}.s3.access-key-id": s3.access_key.get_secret_value(),
        f"{cat}.s3.secret-access-key": s3.secret_key.get_secret_value(),
        f"{cat}.client.region": s3.region,
    }
    if spark.ivy_dir:
        conf["spark.jars.ivy"] = spark.ivy_dir
    return conf


def build_session(
    spark: SparkSettings, iceberg: IcebergSettings, s3: S3Settings, app_name: str
) -> "SparkSession":
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in session_conf(spark, iceberg, s3).items():
        builder = builder.config(key, value)
    log.info(
        "starting spark",
        extra={"master": spark.master, "catalog": iceberg.catalog, "uri": iceberg.rest_uri},
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session

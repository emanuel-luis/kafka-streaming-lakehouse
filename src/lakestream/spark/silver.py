"""Silver transforms: validation, dedupe and projections. Pure, batch and streaming alike."""

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lakestream import contracts
from lakestream.contracts import REJECT_REASONS, REQUIRED_FIELDS, WATERMARK_DELAY

SILVER_COLUMNS = (
    "event_id",
    "event_type",
    "event_time",
    "user_id",
    "session_id",
    "page_url",
    "product_id",
    "quantity",
    "unit_price",
    "order_id",
    "amount",
    "currency",
    "payment_method",
    "payment_status",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
)
REJECT_COLUMNS = (
    "event_id",
    "event_type",
    "event_time",
    "user_id",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
)


def _full_match(column: str, pattern: str) -> Column:
    """True only when the whole value matches; null values give False (like `re.fullmatch`)."""
    return F.coalesce(F.col(column).rlike(rf"^(?:{pattern})\z"), F.lit(False))


def _is_blank(column: str) -> Column:
    return F.col(column).isNull() | (F.col(column) == "")


def _failures() -> list[Column]:
    """One boolean per reject reason, in `REJECT_REASONS` order; mirrors `contracts.validate`."""
    missing = _is_blank("user_id") | _is_blank("session_id")
    for event_type, fields in REQUIRED_FIELDS.items():
        missing |= (F.col("event_type") == event_type) & reduce(
            lambda a, b: a | b, (F.col(f).isNull() for f in fields)
        )
    quantity = F.col("quantity")
    non_positive = [F.col(f).isNotNull() & (F.col(f) <= 0) for f in ("unit_price", "amount")]
    currency, status = F.col("currency"), F.col("payment_status")
    failures = [
        ~_full_match("event_id", "(?i)" + contracts.UUID_RE.pattern),
        ~F.coalesce(F.col("event_type").isin(*contracts.EVENT_TYPES), F.lit(False)),
        missing,
        quantity.isNotNull() & ((quantity < 1) | (quantity > 100)),
        non_positive[0] | non_positive[1],
        currency.isNotNull() & ~_full_match("currency", contracts.CURRENCY_RE.pattern),
        status.isNotNull() & ~status.isin(*contracts.PAYMENT_STATUSES),
    ]
    if len(failures) != len(REJECT_REASONS):
        raise AssertionError("one failure expression per reject reason")
    return failures


def validate(df: DataFrame) -> DataFrame:
    """Add `reject_reason`: the first rule that fails, or null for a valid row."""
    reason = F.lit(None).cast("string")
    for text, failed in reversed(list(zip(REJECT_REASONS, _failures(), strict=True))):
        reason = F.when(failed, F.lit(text)).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def dedupe(df: DataFrame, delay: str = WATERMARK_DELAY) -> DataFrame:
    """Drop repeated `event_id`s seen within the watermark (state is bounded by it)."""
    if not df.isStreaming:  # dropDuplicatesWithinWatermark is rejected on batch frames
        return df.dropDuplicates(["event_id"])
    return df.withWatermark("event_time", delay).dropDuplicatesWithinWatermark(["event_id"])


def project_silver(df: DataFrame) -> DataFrame:
    # Phase 5 adds device_type here.
    return df.select(*SILVER_COLUMNS, F.current_timestamp().alias("processed_at"))


def project_rejects(df: DataFrame) -> DataFrame:
    record = F.to_json(F.struct(*[c for c in df.columns if c != "reject_reason"]))
    return df.select(
        *REJECT_COLUMNS[:4],
        "reject_reason",
        record.alias("record_json"),
        *REJECT_COLUMNS[4:],
        F.current_timestamp().alias("processed_at"),
    )


def silver_events(bronze: DataFrame) -> DataFrame:
    valid = validate(bronze).where(F.col("reject_reason").isNull())
    return project_silver(dedupe(valid))


def silver_rejects(bronze: DataFrame) -> DataFrame:
    return project_rejects(validate(bronze).where(F.col("reject_reason").isNotNull()))

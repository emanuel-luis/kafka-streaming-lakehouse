"""Environment-driven settings, one class per variable prefix. Defaults are host-side."""

from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class KafkaSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "127.0.0.1:19092"
    topic: str = "shop.events"
    dlq_topic: str = "shop.events.dlq"


class SchemaRegistrySettings(_Base):
    model_config = SettingsConfigDict(env_prefix="SCHEMA_REGISTRY_")

    url: str = "http://127.0.0.1:18081"


class ProducerSettings(_Base):
    """Defaults of every `produce` option (`PRODUCER_<OPTION>`)."""

    model_config = SettingsConfigDict(env_prefix="PRODUCER_")

    sessions: int = 400
    users: int = 250
    seed: int = 42
    base_time: str = "2026-01-01T00:00:00Z"
    span_minutes: int = 90
    rate: float = 0.0
    duplicate_rate: float = 0.02
    late_rate: float = 0.03
    late_max_seconds: int = 180
    malformed: int = 10
    invalid: int = 14
    manifest: Path | None = None


class S3Settings(_Base):
    model_config = SettingsConfigDict(env_prefix="S3_")

    endpoint: str = "http://127.0.0.1:19000"
    access_key: SecretStr = SecretStr("lake_admin")
    secret_key: SecretStr = SecretStr("lake_secret_pw")
    region: str = "us-east-1"
    bucket: str = "lakehouse"


class IcebergSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="ICEBERG_")

    rest_uri: str = "http://127.0.0.1:18181"
    warehouse: str = "s3://lakehouse/warehouse"
    catalog: str = "lake"


class ConsumerSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="CONSUMER_")

    group_id: str = "bronze-ingest"
    batch_max_messages: int = 2000
    batch_max_seconds: float = 5
    write_retries: int = 5
    bronze_table: str = "bronze.events"
    heartbeat_file: Path = Path("/tmp/consumer-heartbeat")

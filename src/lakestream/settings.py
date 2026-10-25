"""Environment-driven settings, one class per variable prefix. Defaults are host-side."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class KafkaSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:19092"
    topic: str = "shop.events"
    dlq_topic: str = "shop.events.dlq"


class SchemaRegistrySettings(_Base):
    model_config = SettingsConfigDict(env_prefix="SCHEMA_REGISTRY_")

    url: str = "http://localhost:18081"


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

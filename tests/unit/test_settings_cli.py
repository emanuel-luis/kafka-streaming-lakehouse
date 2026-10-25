import os
import re

import pytest
from typer.testing import CliRunner

from lakestream.cli import app
from lakestream.runmeta import get_run_id, new_run_id
from lakestream.settings import (
    ConsumerSettings,
    IcebergSettings,
    KafkaSettings,
    ProducerSettings,
    S3Settings,
    SchemaRegistrySettings,
)


def test_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [
        n
        for n in os.environ
        if n.startswith(("KAFKA_", "SCHEMA_REGISTRY_", "PRODUCER_", "S3_", "ICEBERG_", "CONSUMER_"))
    ]:
        monkeypatch.delenv(name)
    k = KafkaSettings(_env_file=None)
    assert k.bootstrap_servers == "localhost:19092"
    assert (k.topic, k.dlq_topic) == ("shop.events", "shop.events.dlq")
    assert SchemaRegistrySettings(_env_file=None).url == "http://localhost:18081"
    p = ProducerSettings(_env_file=None)
    assert (p.sessions, p.users, p.seed, p.span_minutes) == (400, 250, 42, 90)
    assert (p.duplicate_rate, p.late_rate, p.malformed, p.invalid) == (0.02, 0.03, 10, 14)
    s3 = S3Settings(_env_file=None)
    assert (s3.endpoint, s3.region, s3.bucket) == (
        "http://localhost:19000",
        "us-east-1",
        "lakehouse",
    )
    assert s3.access_key.get_secret_value() == "lake_admin"
    assert "lake_secret_pw" not in repr(s3)
    ice = IcebergSettings(_env_file=None)
    assert (ice.rest_uri, ice.warehouse, ice.catalog) == (
        "http://localhost:18181",
        "s3://lakehouse/warehouse",
        "lake",
    )
    c = ConsumerSettings(_env_file=None)
    assert (c.group_id, c.batch_max_messages, c.batch_max_seconds) == ("bronze-ingest", 2000, 5)
    assert (c.write_retries, c.bronze_table) == (5, "bronze.events")
    assert c.heartbeat_file.as_posix() == "/tmp/consumer-heartbeat"


def test_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCER_SEED", "7")
    assert ProducerSettings(_env_file=None).seed == 7


def test_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", new_run_id())
    monkeypatch.setenv("RUN_ID", "fixed")
    assert get_run_id() == "fixed"


@pytest.mark.parametrize("args", [[], ["produce"], ["register-schemas"], ["consume"], ["lag"]])
def test_help(args: list[str]) -> None:
    result = CliRunner().invoke(app, [*args, "--help"])
    assert result.exit_code == 0
    assert "Usage" in result.output


def test_produce_rejects_long_span() -> None:
    result = CliRunner().invoke(app, ["produce", "--span-minutes", "151"])
    assert result.exit_code == 2


def test_consume_hides_the_test_hook() -> None:
    result = CliRunner().invoke(app, ["consume", "--help"])
    assert "--exit-when-idle" in result.output
    assert "--crash-after-write-batch" not in result.output

import os
import re

import pytest
from typer.testing import CliRunner

from lakestream.cli import app
from lakestream.runmeta import get_run_id, new_run_id
from lakestream.settings import KafkaSettings, ProducerSettings, SchemaRegistrySettings


def test_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [
        n for n in os.environ if n.startswith(("KAFKA_", "SCHEMA_REGISTRY_", "PRODUCER_"))
    ]:
        monkeypatch.delenv(name)
    k = KafkaSettings(_env_file=None)
    assert k.bootstrap_servers == "127.0.0.1:19092"
    assert (k.topic, k.dlq_topic) == ("shop.events", "shop.events.dlq")
    assert SchemaRegistrySettings(_env_file=None).url == "http://127.0.0.1:18081"
    p = ProducerSettings(_env_file=None)
    assert (p.sessions, p.users, p.seed, p.span_minutes) == (400, 250, 42, 90)
    assert (p.duplicate_rate, p.late_rate, p.malformed, p.invalid) == (0.02, 0.03, 10, 14)


def test_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCER_SEED", "7")
    assert ProducerSettings(_env_file=None).seed == 7


def test_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", new_run_id())
    monkeypatch.setenv("RUN_ID", "fixed")
    assert get_run_id() == "fixed"


@pytest.mark.parametrize("args", [[], ["produce"], ["register-schemas"]])
def test_help(args: list[str]) -> None:
    result = CliRunner().invoke(app, [*args, "--help"])
    assert result.exit_code == 0
    assert "Usage" in result.output


def test_produce_rejects_long_span() -> None:
    result = CliRunner().invoke(app, ["produce", "--span-minutes", "151"])
    assert result.exit_code == 2

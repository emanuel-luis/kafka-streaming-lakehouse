"""Typer entry point. Each command imports its implementation when it runs."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Kafka to lakehouse demo.")


@app.command("register-schemas")
def register_schemas(
    version: Annotated[
        int, typer.Option(min=1, max=1, help="Schema version (only 1 for now).")
    ] = 1,
) -> None:
    """Set BACKWARD compatibility on shop.events-value and register the schema."""
    from lakestream.contracts import SUBJECT
    from lakestream.registry import COMPATIBILITY, register_schema
    from lakestream.settings import SchemaRegistrySettings

    schema_id = register_schema(SchemaRegistrySettings().url, version)
    typer.echo(f"subject={SUBJECT} version={version} id={schema_id} compatibility={COMPATIBILITY}")


@app.command()
def produce(
    sessions: Annotated[int | None, typer.Option(help="Sessions [400].")] = None,
    users: Annotated[int | None, typer.Option(help="Users [250].")] = None,
    seed: Annotated[int | None, typer.Option(help="Random seed [42].")] = None,
    base_time: Annotated[
        str | None, typer.Option(help="Event-time origin, ISO-8601 [2026-01-01T00:00:00Z].")
    ] = None,
    span_minutes: Annotated[
        int | None, typer.Option(help="Event-time span in minutes, max 150 [90].")
    ] = None,
    rate: Annotated[float | None, typer.Option(help="Records per second, 0 = unpaced [0].")] = None,
    duplicate_rate: Annotated[float | None, typer.Option(help="Duplicate share [0.02].")] = None,
    late_rate: Annotated[float | None, typer.Option(help="Target late share [0.03].")] = None,
    late_max_seconds: Annotated[
        int | None, typer.Option(help="Max send delay of a late event [180].")
    ] = None,
    malformed: Annotated[int | None, typer.Option(help="Malformed records [10].")] = None,
    invalid: Annotated[int | None, typer.Option(help="Avro-valid invalid records [14].")] = None,
    manifest: Annotated[
        Path | None, typer.Option(help="Manifest path, default runs/RUN_ID.json.")
    ] = None,
) -> None:
    """Send a synthetic shop event run to Kafka. Defaults come from PRODUCER_* variables."""
    from lakestream.logging import configure_logging
    from lakestream.producer.generator import GenParams, InjectParams
    from lakestream.producer.run import run_produce
    from lakestream.runmeta import get_run_id
    from lakestream.settings import KafkaSettings, ProducerSettings, SchemaRegistrySettings

    def pick[T](value: T | None, default: T) -> T:
        return default if value is None else value

    s = ProducerSettings()
    run_id = get_run_id()
    configure_logging(run_id)
    try:
        base = datetime.fromisoformat(pick(base_time, s.base_time))
        if base.tzinfo is None:
            base = base.replace(tzinfo=UTC)
        gen = GenParams(
            sessions=pick(sessions, s.sessions),
            users=pick(users, s.users),
            seed=pick(seed, s.seed),
            base_time=base.astimezone(UTC),
            span_minutes=pick(span_minutes, s.span_minutes),
        )
        inj = InjectParams(
            duplicate_rate=pick(duplicate_rate, s.duplicate_rate),
            late_rate=pick(late_rate, s.late_rate),
            late_max_seconds=pick(late_max_seconds, s.late_max_seconds),
            malformed=pick(malformed, s.malformed),
            invalid=pick(invalid, s.invalid),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    code = run_produce(
        gen=gen,
        inj=inj,
        rate=pick(rate, s.rate),
        manifest_path=manifest or s.manifest or Path("runs") / f"{run_id}.json",
        kafka=KafkaSettings(),
        registry_url=SchemaRegistrySettings().url,
        run_id=run_id,
    )
    if code:
        raise typer.Exit(code)

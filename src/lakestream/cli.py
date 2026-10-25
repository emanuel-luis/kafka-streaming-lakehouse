"""Typer entry point. Each command imports its implementation when it runs."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Kafka to lakehouse demo.")


@app.command("register-schemas")
def register_schemas(
    version: Annotated[int, typer.Option(min=1, max=2, help="Schema version to register.")] = 1,
    check_only: Annotated[
        str | None,
        typer.Option(
            metavar="PATH",
            help="Only test this schema file (or packaged schema name) for compatibility.",
        ),
    ] = None,
) -> None:
    """Set BACKWARD on shop.events-value, test compatibility, then register the schema."""
    from lakestream.contracts import SUBJECT
    from lakestream.registry import (
        COMPATIBILITY,
        IncompatibleSchemaError,
        check_schema,
        read_schema,
        register_schema,
    )
    from lakestream.settings import SchemaRegistrySettings

    url = SchemaRegistrySettings().url
    if check_only is not None:
        try:
            schema = read_schema(check_only)
        except FileNotFoundError as exc:
            raise typer.BadParameter(str(exc), param_hint="--check-only") from exc
        if not check_schema(url, schema):
            typer.echo(
                f"incompatible: {check_only} breaks {COMPATIBILITY} compatibility of {SUBJECT}",
                err=True,
            )
            raise typer.Exit(1)
        typer.echo(f"compatible: {check_only} with {SUBJECT} ({COMPATIBILITY})")
        return
    try:
        schema_id = register_schema(url, version)
    except IncompatibleSchemaError as exc:
        typer.echo(f"incompatible: {exc}", err=True)
        raise typer.Exit(1) from exc
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
    schema_version: Annotated[
        int | None, typer.Option(min=1, max=2, help="Avro schema version to send [1].")
    ] = None,
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
        schema_version=pick(schema_version, s.schema_version),
    )
    if code:
        raise typer.Exit(code)


@app.command()
def consume(
    group: Annotated[str | None, typer.Option(help="Consumer group [bronze-ingest].")] = None,
    exit_when_idle: Annotated[
        float | None,
        typer.Option(min=0, help="Flush and exit 0 after this many idle seconds [run forever]."),
    ] = None,
    crash_after_write_batch: Annotated[
        int | None, typer.Option(min=1, hidden=True, help="Test hook: die after the Nth append.")
    ] = None,
) -> None:
    """Land shop.events in the bronze Iceberg table; undecodable records go to the DLQ."""
    from lakestream.consumer.run import run_consume
    from lakestream.logging import configure_logging
    from lakestream.runmeta import get_run_id
    from lakestream.settings import (
        ConsumerSettings,
        IcebergSettings,
        KafkaSettings,
        S3Settings,
        SchemaRegistrySettings,
    )

    run_id = get_run_id()
    configure_logging(run_id)
    cfg = ConsumerSettings()
    if group:
        cfg = cfg.model_copy(update={"group_id": group})
    code = run_consume(
        kafka=KafkaSettings(),
        registry_url=SchemaRegistrySettings().url,
        iceberg=IcebergSettings(),
        s3=S3Settings(),
        cfg=cfg,
        run_id=run_id,
        exit_when_idle=exit_when_idle,
        crash_after_write_batch=crash_after_write_batch,
    )
    if code:
        raise typer.Exit(code)


@app.command()
def lag(
    group: Annotated[str | None, typer.Option(help="Consumer group [bronze-ingest].")] = None,
    wait: Annotated[
        bool, typer.Option(help="Poll until total lag is 0 and every partition is committed.")
    ] = False,
    timeout: Annotated[float, typer.Option(help="Seconds to wait with --wait [300].")] = 300,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Per partition: committed offset, end offset, lag and owner. Exit 1 if --wait times out."""
    from lakestream.ops.lag import run_lag
    from lakestream.settings import ConsumerSettings, KafkaSettings

    kafka = KafkaSettings()
    code = run_lag(
        bootstrap_servers=kafka.bootstrap_servers,
        group=group or ConsumerSettings().group_id,
        topic=kafka.topic,
        wait=wait,
        timeout=timeout,
        as_json=as_json,
    )
    if code:
        raise typer.Exit(code)


@app.command()
def check(
    manifest: Annotated[
        list[Path],
        typer.Option(
            exists=True, dir_okay=False, help="Producer manifest, repeatable; expectations summed."
        ),
    ],
    timeout: Annotated[
        float, typer.Option(help="Seconds to retry until everything matches.")
    ] = 120,
    time_travel: Annotated[
        bool, typer.Option(help="Compare bronze at its first append snapshot with now.")
    ] = True,
) -> None:
    """Reconcile bronze, DLQ, rejects, silver and gold with the manifests (Trino). Exit 1 on a
    mismatch."""
    from lakestream.ops.check import run_check
    from lakestream.settings import KafkaSettings, TrinoSettings

    code = run_check(
        manifests=manifest,
        trino_cfg=TrinoSettings(),
        kafka=KafkaSettings(),
        timeout=timeout,
        with_time_travel=time_travel,
    )
    if code:
        raise typer.Exit(code)


def _spark_session(app_name: str) -> tuple[str, Any]:
    """Run id (logging configured) and a SparkSession for one job."""
    from lakestream.logging import configure_logging
    from lakestream.runmeta import get_run_id
    from lakestream.settings import IcebergSettings, S3Settings, SparkSettings
    from lakestream.spark.session import build_session

    run_id = get_run_id()
    configure_logging(run_id)
    return run_id, build_session(SparkSettings(), IcebergSettings(), S3Settings(), app_name)


@app.command()
def silver(
    trigger: Annotated[
        str,
        typer.Option(help="available-now (drain and exit) or processing-time (run until stopped)."),
    ] = "available-now",
    interval: Annotated[
        str, typer.Option(help="Micro-batch interval with --trigger processing-time.")
    ] = "30 seconds",
) -> None:
    """Stream bronze into silver.events and silver.rejects (Spark)."""
    if trigger not in ("available-now", "processing-time"):
        raise typer.BadParameter("must be available-now or processing-time", param_hint="--trigger")
    from lakestream.settings import SparkSettings
    from lakestream.spark.silver_job import run_silver

    run_id, spark = _spark_session("lakestream-silver")
    try:
        run_silver(
            spark,
            run_id=run_id,
            checkpoint_root=SparkSettings().checkpoint_root,
            trigger=trigger,
            interval=interval,
        )
    finally:
        spark.stop()


@app.command()
def gold() -> None:
    """Recompute every gold table from silver.events (Spark batch)."""
    from lakestream.spark.gold_job import run_gold

    run_id, spark = _spark_session("lakestream-gold")
    try:
        run_gold(spark, run_id=run_id)
    finally:
        spark.stop()


@app.command()
def maintain(
    older_than_minutes: Annotated[
        int, typer.Option(min=0, help="Expire snapshots older than this [60].")
    ] = 60,
    retain_last: Annotated[int, typer.Option(min=1, help="Snapshots always kept [10].")] = 10,
    table: Annotated[
        list[str] | None, typer.Option(help="Table to maintain, repeatable [all].")
    ] = None,
) -> None:
    """Compact, rewrite manifests and expire snapshots; bronze expiry waits for silver."""
    from lakestream.settings import IcebergSettings, SparkSettings
    from lakestream.spark.maintain import ALL_TABLES, run_maintain

    unknown = sorted(set(table or []) - set(ALL_TABLES))
    if unknown:
        raise typer.BadParameter(f"unknown table(s) {unknown}; known: {ALL_TABLES}")
    _, spark = _spark_session("lakestream-maintain")
    try:
        run_maintain(
            spark,
            catalog=IcebergSettings().catalog,
            checkpoint_root=SparkSettings().checkpoint_root,
            older_than_minutes=older_than_minutes,
            retain_last=retain_last,
            tables=list(table or []),
        )
    finally:
        spark.stop()


@app.command("spark-warmup")
def spark_warmup() -> None:
    """Start and stop a session so Ivy resolves the Iceberg packages (image build time)."""
    _, spark = _spark_session("lakestream-warmup")
    spark.stop()

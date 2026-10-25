# CLAUDE.md

Kafka end to end into a lakehouse: Python producer (Avro + Schema Registry) -> Kafka (KRaft) ->
Python consumer group (PyIceberg bronze, DLQ) -> Spark Structured Streaming silver -> Spark batch
gold -> Trino. Iceberg tables on S3-compatible storage through an Iceberg REST catalog. Public
portfolio repo; all data is synthetic.

## Commands

- `make install`: `uv sync --all-extras --locked`.
- `make lint`: ruff check, ruff format --check, mypy strict on `src` (same as the CI lint job).
- `make test-fast`: pytest with the default selection (deselects `spark` and `integration`).
- `make test-integration`: tests against the running compose stack (`make up` first).
- `make up` / `make down`: start the stack (healthchecks, `--wait`; the consumer pulls in every init
  job) / remove it with its volumes and every profile.
- `make lag`: `lag --wait` in the `cli` container (profile `tools`): the consumer group has caught up.
- `make produce`: run the producer once inside the stack; the manifest lands in `/runs`.
- `make ui`: Kafbat UI on http://localhost:18090 (profile `ui`).
- `make silver` / `make gold` / `make maintain`: Spark jobs in the `spark-job` container.
- `make check`: `lakestream check` in the `cli` container (`MANIFESTS="--manifest /runs/run1.json"`).
- CI (`.github/workflows/ci.yml`): `lint` gates `test-fast`, `test-spark` and `e2e` (`make demo`,
  `make test-integration`, `make down`); `UV_LOCKED=1`; actions pinned by SHA. `security.yml` runs
  pip-audit; Dependabot covers actions and uv, not Docker images.
- `make demo`: `scripts/demo.sh`, the whole run from fresh volumes (a stack run: heavy lock).
- Spark tests and jobs never run in Windows Python. On the owner's machine:
  `wsl -d Ubuntu -e bash -lc 'cd /mnt/d/dev/Pessoal/kafka-streaming-lakehouse && UV_PROJECT_ENVIRONMENT=$HOME/.venvs/kafka-streaming-lakehouse make test-spark'`.

## Repo map

- `docs/PLAN.md`: the contract (topics, Avro schema, tables, env vars, CLI, compose services,
  phases). `docs/DECISIONS.md`: decision log with reasons.
- `compose.yaml`: the stack; project name `kafka-streaming-lakehouse`; host ports from `.env`.
- `scripts/create-bucket.sh`: bucket creation run by `s3-init`. Services added in phase 2: `s3`
  (RustFS, ports 19000/19001), `iceberg-rest` (fixture, SQLite WAL on `catalog_data`, port 18181),
  `consumer` (2 replicas) and `cli` (profile `tools`).
- `docker/app.Dockerfile`: `app` target (no JVM) and `spark` target (Temurin 17 JRE, `--extra spark`,
  Iceberg jars resolved into `/opt/spark-ivy` at build time). Compose service `spark-job` (profile
  `tools`, checkpoints on volume `spark_checkpoints`, Spark UI on port 14040).
- `scripts/create-topics.sh`: topic creation run by `kafka-init`. `scripts/demo.sh`: `make demo`.
- `trino/catalog/lake.properties`: Trino Iceberg catalog (REST + native S3, keys from env); service
  `trino` (port 18080, heap via `JAVA_TOOL_OPTIONS`).
- `src/lakestream/`: `cli.py` (Typer, single entry point), `settings.py`, `logging.py`,
  `runmeta.py`, `contracts.py` (event types, reject reasons, watermark), `registry.py`,
  `schemas/*.avsc`, `producer/` (pure generator + Kafka I/O + manifest), `consumer/` (bronze),
  `spark/` (the only package that imports pyspark: `session`, `tables` (every Spark table schema),
  pure `silver`/`gold` transforms, `silver_job`, `gold_job`, `maintain`, `runlog`, and the pure
  `checkpoint` parser), `ops/` (`lag`, pure `expectations` built from manifests, `check` through
  the trino client).
- `tests/unit/` (no network, no JVM), `tests/integration/` (compose stack), `tests/spark/` (WSL/CI).

## Invariants

- Names in `docs/PLAN.md` are contracts: change the Python side and the compose side together.
- Event time comes from a fixed synthetic base, never the wall clock; the same seed produces the
  same events, so silver and gold counts are exact and repeatable.
- The bronze table is append-only, and every bronze append records its Kafka offsets in the
  snapshot summary (`ingest.offsets`); the consumer commits Kafka offsets only after that append.
- `cli`, `settings`, `logging`, `runmeta`, `contracts`, `registry`, `producer`, `consumer` and
  `ops` never import pyspark; `cli.py` imports command modules inside the command functions.
- The producer refuses spans above `MAX_SPAN_MINUTES` (150) so the silver watermark (3 hours)
  never drops a record from a normal run.
- Image tags and Python pins move together with `docs/PLAN.md` and `docs/DECISIONS.md`.

## Conventions

- Python 3.12, uv, ruff (line length 100, `BLE`), mypy strict, pytest with Hypothesis.
- Settings through pydantic-settings; secrets as `SecretStr`, never logged.
- Logs are JSON lines with UTC timestamps and `run_id` / `batch_id`.
- Conventional Commits in English; one PR per phase, stacked.
- Stack and Spark runs on the owner's machine take the shared heavy lock
  (`D:/dev/Pessoal/.locks/heavy`) and end with `make down`.
- Host ports default to the 1xxxx range so the stack can run next to `cdc-streaming-demo`.
- No employer names, hosts or identifiers anywhere in this repo.

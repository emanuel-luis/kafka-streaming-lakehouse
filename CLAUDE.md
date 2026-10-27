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
- `make d2-install`: installs the pinned D2 (`D2_VERSION`, sha256-checked `D2_SHA256`, Linux
  x86_64) into `~/.local/share/d2/v<version>`. `make diagrams`: renders every `docs/diagrams/*.d2`
  (not `_*.d2`) to SVG. `make diagrams-check`: renders, then fails on a stale, missing or orphan
  SVG; exactly what the CI `diagrams` job runs. `make diagrams-watch NAME=<name>`: `d2 --watch`
  live preview; `NAME` is read only from the command line (WSL exports `NAME` as the hostname).
  On the owner's machine render through WSL:
  `wsl -d Ubuntu -e bash -lc 'cd /mnt/d/dev/Pessoal/kafka-streaming-lakehouse && make diagrams'`.
- CI (`.github/workflows/ci.yml`): `lint` gates `test-fast`, `test-spark` and `e2e` (`make demo`,
  `make test-integration`, `make down`); `diagrams` (`make d2-install`, `make diagrams-check`, SVGs
  uploaded as an artifact) runs on its own; `UV_LOCKED=1`; actions pinned by SHA. `security.yml`
  runs pip-audit; Dependabot covers actions and uv, not Docker images.
- `make demo`: `scripts/demo.sh`, the whole run from fresh volumes (a stack run: heavy lock): run 1
  with schema v1, then v2, the rejected v2-breaking check and run 2, then maintain.
- Spark tests and jobs never run in Windows Python. On the owner's machine:
  `wsl -d Ubuntu -e bash -lc 'cd /mnt/d/dev/Pessoal/kafka-streaming-lakehouse && UV_PROJECT_ENVIRONMENT=$HOME/.venvs/kafka-streaming-lakehouse make test-spark'`.

## Repo map

- `docs/PLAN.md`: the contract (topics, Avro schema, tables, env vars, CLI, compose services,
  phases). `docs/DECISIONS.md`: decision log with reasons.
- `docs/diagrams/`: D2 sources and their committed SVGs: `architecture`, `data-flow`,
  `data-model` (every Iceberg table, keys and partitions), `user-flows-event-lifecycle`,
  `user-flows-schema-evolution`, `user-flows-make-demo`, `user-flows-consumer-restart`.
  `_style.d2` (a copy of the portfolio pilot's) holds the `d2-config` (ELK, theme 0 plus dark
  theme 200, pad, color overrides) and the classes every diagram uses.
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
  `schemas/*.avsc` (v1, v2 with `device_type`, and v2-breaking, which only shows the registry
  rejecting it), `producer/` (pure generator + Kafka I/O + manifest), `consumer/` (bronze, with
  `union_by_name` evolution to each new writer schema),
  `spark/` (the only package that imports pyspark: `session`, `tables` (every Spark table schema),
  pure `silver`/`gold` transforms, `silver_job`, `gold_job`, `maintain`, `runlog`, and the pure
  `checkpoint` parser), `ops/` (`lag`, `dlq_show` (the `dlq` command), pure `expectations` built
  from manifests, `check` through the trino client).
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
- Diagrams are D2 only (no Mermaid). Each `.d2` starts with `...@_style`, uses its classes
  (`process`, `source`, `store`, `serving`, `actor`, `external`, `check`; `meta` for dashed edges)
  and never sets a fill color: explicit colors do not switch in dark mode. Legends are hand-made
  containers (`class: legend`). ELK turns every edge label into a layer, so keep text in node
  labels and edges top-down; move a container label (`label.near`) when an edge crosses it. About
  25 nodes and 1,300 px wide at most: GitHub scales images down to its ~980 px column. A diagram
  names only what the code has; when a contract changes in `docs/PLAN.md`, update the diagrams in
  the same PR. Commit the `.d2` and the `.svg` together; never edit an SVG by hand, and edit `.d2`
  files with an editor, not `sed` (it mangles `\n` inside D2 strings).

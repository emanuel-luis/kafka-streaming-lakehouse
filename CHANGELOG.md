# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Schema evolution: `ShopEvent` v2 adds an optional `device_type`. `register-schemas --version 2`
  tests compatibility before registering, and `--check-only PATH` only tests a schema;
  `shop_event.v2-breaking.avsc` shows the registry rejecting a required field with no default.
- `lakestream produce --schema-version 1|2`; v2 events carry a seeded device per session and the
  manifest counts them (`device_type_events`).
- The consumer adds the columns of each new writer schema to bronze with `union_by_name` inside
  the append retry; silver gains `device_type` (`ALTER TABLE ... ADD COLUMN` on an older table).
- `lakestream dlq --limit N`: dead letters with their headers and a hex preview, read without
  committing.
- `lakestream check` reconciles `device_type` against the v2 manifests and, with two or more
  manifests, reads gold at the previous gold run's snapshot.
- `make demo` runs a second, schema v2 producer run and checks both manifests.
- GitHub Actions: `ci.yml` (lint, then unit, Spark with Java 17 and an end-to-end job that runs
  `make demo` and `make test-integration`), `security.yml` (pip-audit over the locked
  dependencies) and weekly grouped Dependabot updates for actions and uv. Actions are pinned by SHA.
- Trino 484 (`trino` service, port 18080) with the Iceberg catalog `lake` over the REST catalog
  and native S3; part of `make up`. The `cli` service waits for Trino and mounts `/runs`.
- `lakestream check`: reconciles bronze, the DLQ, rejects, silver and gold with one or more
  producer manifests through Trino (DLQ from Kafka), with bronze time travel; retries until
  everything matches or `--timeout`, exits 1 on a mismatch. `make check`.
- `make demo` (`scripts/demo.sh`): fresh stack, produce with seed 42, lag, silver, gold, check,
  maintain and check again, printing service status and logs on failure.
- README with the architecture, quickstart and Trino queries.
- Docker Compose stack: Kafka (KRaft, single node), Schema Registry with BACKWARD compatibility,
  Kafka UI (profile `ui`), and idempotent init containers for topics and the v1 schema.
- `lakestream produce`: seeded synthetic shop events (Avro, Confluent wire format) with
  duplicates, late, malformed and invalid injections, and a JSON manifest per run.
- `lakestream register-schemas` and the `ShopEvent` v1 Avro schema.
- Compose services for phase 2: RustFS object storage with an idempotent bucket init, the Iceberg
  REST catalog (SQLite in WAL mode on a named volume, S3FileIO), a two-replica `consumer` with a
  heartbeat healthcheck, and a `cli` service (profile `tools`). `make up` is a plain
  `up --wait`; new `make lag`.
- Consumer polls at most the room left in the batch, so `CONSUMER_BATCH_MAX_MESSAGES` is a hard cap.
- Bronze appends are fenced against offsets another writer of the group already wrote; the table
  sets `commit.retry.num-retries=0` so every commit conflict reaches the fence.
- `lakestream lag --wait` treats a partition that never received a message as caught up.
- Spark job layer: `lakestream silver` (two Iceberg streaming queries from bronze into
  `silver.events` and `silver.rejects`, `available-now` or `processing-time`), `lakestream gold`
  (recompute of the three gold tables from one silver snapshot), `lakestream maintain`
  (compaction, manifest rewrite, snapshot expiry; bronze expiry never passes the initial or the
  resume snapshot of the silver queries) and `lakestream spark-warmup`. Every job run appends a row to
  `ops.job_runs`.
- `spark` image target (Temurin 17 JRE, Iceberg packages baked in) and the `spark-job` compose
  service (profile `tools`); `make silver`, `make gold`, `make maintain`.
- After a restart, the consumer commits the resume offset taken from bronze snapshots when it is
  ahead of the group's Kafka commit, so lag reaches 0 without new records.
- Tooling: uv, ruff, mypy, pytest, Makefile, `.env.example` and the `app` Docker image.

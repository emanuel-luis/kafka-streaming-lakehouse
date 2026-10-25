# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
  (compaction, manifest rewrite, snapshot expiry; bronze expiry never passes the snapshot the
  silver queries resume from) and `lakestream spark-warmup`. Every job run appends a row to
  `ops.job_runs`.
- `spark` image target (Temurin 17 JRE, Iceberg packages baked in) and the `spark-job` compose
  service (profile `tools`); `make silver`, `make gold`, `make maintain`.
- Tooling: uv, ruff, mypy, pytest, Makefile, `.env.example` and the `app` Docker image.

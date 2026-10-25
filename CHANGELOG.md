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
- Tooling: uv, ruff, mypy, pytest, Makefile, `.env.example` and the `app` Docker image.

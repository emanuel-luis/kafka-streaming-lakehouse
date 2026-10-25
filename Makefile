SHELL := bash
.DEFAULT_GOAL := help

.PHONY: help install lint fmt test-fast test-integration test up down ps logs ui produce

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync

lint: ## Ruff check, ruff format check, mypy
	uv run ruff check
	uv run ruff format --check
	uv run mypy

fmt: ## Format and apply safe lint fixes
	uv run ruff format
	uv run ruff check --fix

test-fast: ## Unit tests, no stack needed
	uv run pytest

test-integration: ## Integration tests (stack must be up)
	uv run pytest -m integration

test: test-fast ## Everything that runs without the stack

# `up --wait` treats a one-shot service as "completed successfully" only when an active service
# depends on it with that condition; otherwise it waits for "running" and fails on the exit.
# Until the consumer (phase 2) depends on the init jobs, run them with `run --rm`, which returns
# their exit code. `--build` on `run` rebuilds lakestream-app:local (no `build:` on the `up`
# services), so a src/ change never runs a stale image.
up: ## Start the stack in the background, wait until healthy, run the init jobs
	docker compose up -d --build --wait kafka schema-registry
	docker compose run --rm kafka-init
	docker compose run --rm --build schema-init

down: ## Stop the stack and delete its volumes
	docker compose --profile tools --profile ui down -v --remove-orphans

ps: ## Show service status
	docker compose ps

logs: ## Follow the logs of every service
	docker compose logs -f --tail 100

ui: ## Start Kafka UI (http://localhost:18090)
	docker compose --profile ui up -d kafka-ui

ARGS ?= --manifest /runs/run1.json
produce: ## Run the producer (override options with ARGS="...")
	docker compose run --rm --build producer produce $(ARGS)

SHELL := bash
.DEFAULT_GOAL := help
# Git Bash on Windows would rewrite container paths such as /runs/run1.json into Windows paths.
export MSYS_NO_PATHCONV := 1

.PHONY: help install lint fmt test-fast test-spark test-integration test up down ps logs ui produce lag silver gold maintain check demo

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync --all-extras

lint: ## Ruff check, ruff format check, mypy
	uv run --all-extras ruff check
	uv run --all-extras ruff format --check
	uv run --all-extras mypy

fmt: ## Format and apply safe lint fixes
	uv run ruff format
	uv run ruff check --fix

test-fast: ## Unit tests, no stack needed
	uv run pytest

test-spark: ## Spark tests (WSL or CI, never Windows Python)
	uv run --extra spark pytest -m spark

test-integration: ## Integration tests (stack must be up)
	uv run pytest -m integration

test: test-fast ## Everything that runs without the stack

# The consumer depends on every init job with `service_completed_successfully`, so `--wait` treats
# them as one-shots and fails if one exits non-zero (docs/DECISIONS.md 19).
up: ## Start the stack in the background and wait until it is healthy
	docker compose up -d --build --wait

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

lag: ## Wait until the consumer group has caught up with the topic
	docker compose run --rm --build cli lag --wait

# Spark jobs run in the spark-job container (profile tools); --service-ports publishes the Spark UI
# on http://localhost:14040 while a job runs.
SPARK_RUN := docker compose run --rm --build --service-ports spark-job

silver: ## Stream bronze into silver until caught up (available-now)
	$(SPARK_RUN) silver --trigger available-now

gold: ## Recompute the gold tables from silver
	$(SPARK_RUN) gold

MAINTAIN_ARGS ?=
maintain: ## Compact and expire snapshots (options with MAINTAIN_ARGS="...")
	$(SPARK_RUN) maintain $(MAINTAIN_ARGS)

MANIFESTS ?= --manifest /runs/run1.json
check: ## End-to-end check through Trino (manifests with MANIFESTS="--manifest ...")
	docker compose run --rm --build cli check $(MANIFESTS)

demo: ## Fresh stack, produce, silver, gold, check, maintain, check again
	bash scripts/demo.sh

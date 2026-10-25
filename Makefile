SHELL := bash
.DEFAULT_GOAL := help

.PHONY: help install lint fmt test-fast test-integration test up down ps logs ui produce lag

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
	docker compose run --rm producer produce $(ARGS)

lag: ## Wait until the consumer group has caught up with the topic
	docker compose run --rm cli lag --wait

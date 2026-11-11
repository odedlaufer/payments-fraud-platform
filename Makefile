.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE_PROFILES ?= core,lake
export COMPOSE_PROFILES

.PHONY: help setup lint fmt typecheck test check clean up down nuke ps logs health gate test-integration migrate migrate-status migrate-rollback new-migration connect-plugins connectors connector-status

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install dev dependencies and git hooks
	uv sync --frozen
	GIT_CONFIG_GLOBAL=/dev/null uv run pre-commit install
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")

lint: ## Run all pre-commit hooks on all files
	uv run pre-commit run --all-files

fmt: ## Auto-format Python
	uv run ruff format .
	uv run ruff check --fix .

typecheck: ## Run mypy
	uv run mypy tests

test: ## Run unit tests
	uv run pytest

test-integration: ## Run integration tests against the running stack (make up first)
	uv run pytest -m integration

check: lint typecheck test ## Run everything CI runs

clean: ## Remove caches
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

up: ## Start the stack (COMPOSE_PROFILES=core,lake) and wait until healthy
	@test -f .env || (echo ".env missing: run make setup" && exit 1)
	docker compose up -d
	scripts/health.sh

down: ## Stop the stack, keep data
	docker compose down

nuke: ## Stop the stack and delete all data volumes
	docker compose down -v --remove-orphans

ps: ## Show service status
	docker compose ps -a

logs: ## Tail logs (SERVICE=name for one service)
	docker compose logs -f --tail=200 $(SERVICE)

health: ## Check health of running services
	scripts/health.sh 30

gate: ## Phase 0 gate: Trino writes and reads an Iceberg table via Lakekeeper on SeaweedFS
	scripts/phase0-gate.sh

migrate: ## Apply pending OLTP migrations
	docker compose run --rm postgres-migrate

migrate-status: ## List applied and pending OLTP migrations
	docker compose run --rm postgres-migrate --no-dump-schema status

migrate-rollback: ## Roll back the latest OLTP migration
	docker compose run --rm postgres-migrate --no-dump-schema rollback

new-migration: ## Create an empty migration: make new-migration NAME=add_x
	@test -n "$(NAME)" || (echo "usage: make new-migration NAME=snake_case_name" && exit 1)
	@f=postgres/migrations/$$(date -u +%Y%m%d%H%M%S)_$(NAME).sql; \
	printf -- '-- migrate:up\n\n-- migrate:down\n' > $$f; echo "created $$f"

connect-plugins: ## List Kafka Connect connector and converter plugins
	@curl -fsS "http://localhost:$${CONNECT_PORT:-8083}/connector-plugins?connectorsOnly=false" \
	  | uv run python -c "import json,sys; [print(p['type'].ljust(10), p['class'], p.get('version','')) for p in json.load(sys.stdin)]"

connectors: ## Register or update connectors from connect/connectors/*.json
	docker compose run --rm connect-init

connector-status: ## Show Debezium connector and task state
	@curl -fsS "http://localhost:$${CONNECT_PORT:-8083}/connectors/payments-postgres/status" \
	  | uv run python -c "import json,sys; s=json.load(sys.stdin); print('connector', s['connector']['state']); [print('task', t['id'], t['state']) for t in s['tasks']]"

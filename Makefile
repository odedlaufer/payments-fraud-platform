.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE_PROFILES ?= core,lake,stream
export COMPOSE_PROFILES

.PHONY: help setup lint fmt typecheck test check clean up down nuke ps logs health gate test-integration migrate migrate-status migrate-rollback new-migration connect-plugins connectors connector-status seed sim cdc-reconcile phase1-gate flink-check lock-jars gen-bronze flink-jobs bronze-reconcile chaos-ch1-bronze dbt-build dbt-run dbt-test silver-reconcile phase2-gate

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
	uv run mypy tests simulator/src scripts

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

seed: ## Insert simulator reference data (ARGS="--customers 2000 --merchants 300 --seed 42")
	uv run simulator seed $(ARGS)

sim: ## Run the simulator in real time (SPEED=real min per sim day, TPS=tx per second, ARGS=...)
	uv run simulator run --minutes-per-day $(or $(SPEED),10) --tps $(or $(TPS),20) $(ARGS)

cdc-reconcile: ## Compare every Postgres table with its CDC topic, key by key
	uv run python scripts/cdc_reconcile.py

phase1-gate: ## Phase 1 gate: >=10,000 simulated transactions, then CDC must match Postgres exactly
	@uv run simulator seed 2>/dev/null || echo "reference data already present"
	uv run simulator run --fast --max-transactions 10000 --change-rate 5
	uv run python scripts/cdc_reconcile.py

flink-check: ## ADR-002 verification 2: Flink writes Iceberg via Lakekeeper, Trino reads it
	scripts/flink-iceberg-check.sh

lock-jars: ## Regenerate flink/jars.lock from flink/jars.txt (verifies Maven SHA-1, pins SHA-256)
	scripts/lock-jars.sh flink/jars.txt > flink/jars.lock.tmp && mv flink/jars.lock.tmp flink/jars.lock

gen-bronze: ## Regenerate bronze DDL (Trino) and the bronze Flink job from the Postgres schema
	uv run python scripts/gen_bronze.py

flink-jobs: ## Submit long-running Flink jobs that are not already running
	docker compose run --rm flink-jobs

bronze-reconcile: ## Every CDC event is in bronze exactly once (waits for the 60 s commits)
	uv run python scripts/bronze_reconcile.py

chaos-ch1-bronze: ## CH-1 (bronze): SIGKILL the TaskManager mid-run, then reconcile bronze
	chaos/ch1-bronze.sh

dbt-build: ## dbt: run all models and tests (staging views, silver MERGE)
	cd dbt && uv run dbt build --profiles-dir .

dbt-run: ## dbt: run models only
	cd dbt && uv run dbt run --profiles-dir .

dbt-test: ## dbt: run tests only
	cd dbt && uv run dbt test --profiles-dir .

silver-reconcile: ## DQ-08: Postgres and silver agree row for row; records to lake.ops.dq_results
	uv run python scripts/silver_reconcile.py

phase2-gate: ## Phase 2 gate: bronze exact, dbt (silver + SCD2) passes, silver == Postgres, >= 3 sim days
	scripts/phase2-gate.sh

.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE_PROFILES ?= core,lake
export COMPOSE_PROFILES

.PHONY: help setup lint fmt typecheck test check clean up down nuke ps logs health gate

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

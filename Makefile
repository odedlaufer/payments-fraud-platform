.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help setup lint fmt typecheck test check clean

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

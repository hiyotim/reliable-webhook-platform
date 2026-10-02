# Developer entry points for the reliable webhook platform.
# Run `make help` for the target list.
#
# The project venv is expected at .venv (that is what `uv venv` creates and
# what the repo ships with); override VENV=... PY=... for another layout.

SHELL := /bin/bash

VENV ?= .venv
PY ?= $(VENV)/bin/python
UV ?= uv
COMPOSE ?= docker compose
VERIFY_FILES := -f docker-compose.yml -f docker-compose.verify.yml

.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck test migrate api worker receiver up down logs e2e local-db clean

help: ## List available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install: ## Create .venv if needed, then install the project with dev extras
	@test -x $(PY) || $(UV) venv $(VENV)
	$(UV) pip install --python $(PY) -e ".[dev]"

lint: ## Lint with ruff
	$(PY) -m ruff check .

format: ## Auto-format with ruff
	$(PY) -m ruff format .

typecheck: ## Type-check with mypy
	$(PY) -m mypy

test: ## Run the test suite (TEST_DATABASE_URL or embedded PostgreSQL)
	$(PY) -m pytest -q

migrate: ## Apply Alembic migrations to DATABASE_URL
	$(PY) -m alembic upgrade head

api: ## Run the HTTP API locally
	$(PY) -m webhooks.api

worker: ## Run one worker locally
	$(PY) -m webhooks.worker

receiver: ## Run the reference test receiver locally (port 9000)
	$(PY) -m webhooks.testing.receiver

up: ## Build and start the Compose stack (postgres, migrate, api, 2 workers)
	$(COMPOSE) up -d --build

down: ## Stop the Compose stack (keeps the postgres volume)
	$(COMPOSE) down --remove-orphans

logs: ## Follow Compose logs
	$(COMPOSE) logs -f --tail=100

e2e: ## Build + start the stack, then run the end-to-end verification
	$(COMPOSE) $(VERIFY_FILES) up -d --build
	$(COMPOSE) $(VERIFY_FILES) run --rm verify

local-db: ## Start a local PostgreSQL via pgserver (no Docker) and print DATABASE_URL
	$(PY) scripts/local_postgres.py

clean: ## Remove caches and build artifacts (leaves .venv alone)
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist ./*.egg-info
	find . -path ./$(VENV) -prune -o -name '__pycache__' -type d -print0 | xargs -0 -r rm -rf
	find . -path ./$(VENV) -prune -o -name '*.py[co]' -type f -print0 | xargs -0 -r rm -f

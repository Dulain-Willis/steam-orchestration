# The whole local dev loop. See README.md ("Local development").
SHELL := /bin/bash
COMPOSE := docker compose
# The containers bind-mount host directories, so run them as you: files they
# write (logs/, the dbt target/ in your steam-analytics checkout) end up
# yours instead of uid 50000's.
export AIRFLOW_UID := $(shell id -u)

.PHONY: up down logs trigger test

# Bring up local Airflow and report honestly whether the DAG parsed.
up:
	@mkdir -p logs
	$(COMPOSE) up -d --wait
	@echo "Waiting for the dag-processor to parse steam_dbt_dag (Cosmos runs dbt ls, ~1 min)..."
	@for _ in $$(seq 60); do \
	  errors=$$($(COMPOSE) exec -T airflow-scheduler airflow dags list-import-errors 2>/dev/null); \
	  case "$$errors" in *steam_dbt_dag*) printf '%s\n' "$$errors"; exit 1;; esac; \
	  if $(COMPOSE) exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -q steam_dbt_dag; then \
	    echo "steam_dbt_dag parsed, no import errors. UI: http://localhost:8080 (no login)"; \
	    exit 0; \
	  fi; \
	  sleep 5; \
	done; \
	echo "steam_dbt_dag never appeared — check 'make logs'" >&2; exit 1

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --follow --tail 50

# Kick off a run; watch it in the UI or with `make logs`.
trigger:
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger steam_dbt_dag

# DAG validity tests. No Docker and no Snowflake. The venv is on PATH because
# Cosmos shells out to a `dbt` executable it looks up there.
test: .venv
	PATH="$(CURDIR)/.venv/bin:$$PATH" .venv/bin/python -m pytest tests/ -v

.venv:
	uv venv --python 3.12 .venv
	uv pip install --python .venv/bin/python -r requirements-dev.txt

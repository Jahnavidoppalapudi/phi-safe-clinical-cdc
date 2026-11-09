.PHONY: help env setup up down ps logs connector topics cdc-peek ehr test test-db lint format reset clean

help:            ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

env:             ## Create .env with random secrets (only if it does not exist)
	@if [ -f .env ]; then echo ".env already exists, leaving it alone"; else \
		cp .env.example .env; \
		for key in EHR_DB_PASSWORD CDC_PASSWORD DEID_SECRET; do \
			sed -i.bak "s/^$$key=CHANGE_ME/$$key=$$(openssl rand -hex 32)/" .env; \
		done; rm -f .env.bak; echo "created .env with random secrets"; fi

setup: env       ## Create local virtual env with dev tools
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install

up: env          ## Build and start the PHI zone: EHR database, simulator, Redpanda, Debezium
	docker compose up -d --build

down:            ## Stop all services (data is kept)
	docker compose down

ps:              ## Show running services
	docker compose ps

logs:            ## Follow the simulator
	docker compose logs -f simulator

connector:       ## Debezium connector status and registration log
	@docker compose logs connector-setup | tail -5
	@curl -s localhost:8083/connectors/ehr-cdc/status | python3 -m json.tool

topics:          ## CDC topics with message counts
	docker compose exec redpanda rpk topic list --detailed

cdc-peek:        ## Last 3 raw change events for patients (contains synthetic PHI)
	docker compose exec redpanda rpk topic consume ehr.public.patients --offset -3 --num 3 --format '%v\n'

ehr:             ## SQL prompt on the EHR database (PHI zone)
	docker compose exec ehr-db psql -U ehr_owner -d ehr

test:            ## Unit tests (database tests skip without TEST_DATABASE_URL)
	.venv/bin/pytest -v

test-db:         ## All tests, using the running EHR PostgreSQL for throwaway test databases
	@set -a; . ./.env; set +a; \
	TEST_DATABASE_URL=postgresql://ehr_owner:$$EHR_DB_PASSWORD@localhost:15433/postgres .venv/bin/pytest -v

lint:            ## Check code style
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

format:          ## Auto-format code
	.venv/bin/ruff format . && .venv/bin/ruff check --fix .

reset:           ## Stop everything AND delete all data
	docker compose down -v

clean:           ## Remove caches
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache

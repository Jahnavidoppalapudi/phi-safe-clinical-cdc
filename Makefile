.PHONY: help env setup up down ps logs plogs connector topics cdc-peek ehr analyst metrics audit leak-scan test test-db lint format reset clean

help:            ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

SECRETS = EHR_DB_PASSWORD CDC_PASSWORD DEID_SECRET ANALYTICS_DB_PASSWORD ANALYST_PASSWORD

env:             ## Create .env, or add missing secrets to it (never changes existing values)
	@test -f .env || { cp .env.example .env; echo "created .env"; }
	@for key in $(SECRETS); do \
		if ! grep -q "^$$key=" .env; then \
			echo "$$key=$$(openssl rand -hex 32)" >> .env; echo "added $$key"; \
		elif grep -q "^$$key=CHANGE_ME" .env; then \
			sed -i.bak "s/^$$key=CHANGE_ME/$$key=$$(openssl rand -hex 32)/" .env; rm -f .env.bak; \
			echo "generated $$key"; \
		fi; \
	done

setup: env       ## Create local virtual env with dev tools
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install

up: env          ## Build and start everything: PHI zone, processor, analytics zone
	docker compose up -d --build

down:            ## Stop all services (data is kept)
	docker compose down

ps:              ## Show running services
	docker compose ps

logs:            ## Follow the simulator
	docker compose logs -f simulator

plogs:           ## Follow the de-identification processor
	docker compose logs -f deid-processor

connector:       ## Debezium connector status and registration log
	@docker compose logs connector-setup | tail -5
	@curl -s localhost:8083/connectors/ehr-cdc/status | python3 -m json.tool

topics:          ## CDC topics with message counts
	docker compose exec redpanda rpk topic list --detailed

cdc-peek:        ## Last 3 raw change events for patients (contains synthetic PHI)
	docker compose exec redpanda rpk topic consume ehr.public.patients --offset -3 --num 3 --format '%v\n'

ehr:             ## SQL prompt on the EHR database (PHI zone)
	docker compose exec ehr-db psql -U ehr_owner -d ehr

analyst:         ## SQL prompt as the analyst (consent-gated views only)
	@set -a; . ./.env; set +a; \
	docker compose exec -e PGPASSWORD=$$ANALYST_PASSWORD analytics-db psql -h localhost -U analyst -d analytics

metrics:         ## Readmissions, length of stay and lab abnormality rates (de-identified)
	@for view in v_readmissions_30d v_length_of_stay v_abnormal_labs; do \
		docker compose exec analytics-db psql -U analytics_owner -d analytics -c "select * from $$view order by 1"; \
	done

audit:           ## Recent privacy actions: purges, deletions, unclassified columns, leak scans
	docker compose exec analytics-db psql -U analytics_owner -d analytics -c "select logged_at, action, left(coalesce(patient_token, ''), 16) as patient, table_name, left(details::text, 70) as details from audit_log order by audit_id desc limit 20"

leak-scan:       ## Search the analytics zone for identifier patterns
	docker compose exec deid-processor python -m src.leak_scan

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

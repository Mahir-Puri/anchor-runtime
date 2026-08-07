.PHONY: help up down db test lint demo chaos load api worker migrate clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

db: ## Start Postgres only (what the tests need)
	docker compose up -d postgres
	@until docker compose exec -T postgres pg_isready -U postgres -d anchor >/dev/null 2>&1; do sleep 0.5; done
	@echo "postgres ready on 5432"

up: ## Start Postgres, the API and two workers
	docker compose up --build -d
	@echo "api on http://localhost:8000  (try: curl localhost:8000/healthz)"

down: ## Stop everything and drop the volume
	docker compose down -v

migrate: ## Apply the schema
	anchor migrate

test: db ## Run the full suite against a real Postgres
	pytest -q

lint: ## Ruff
	ruff check src tests scripts

demo: db migrate ## Submit one refund run, work it, print the event log
	anchor demo --payment-id pay_002 --amount-cents 129900

chaos: db migrate ## SIGKILL a worker mid-refund and verify exactly-once
	python scripts/chaos_kill.py --kills 3

load: db migrate ## 200 runs across 4 workers, with percentiles
	python scripts/loadtest.py --runs 200 --workers 4

api: ## Serve the control plane locally
	anchor api --reload

worker: ## Run one worker locally
	anchor worker -v

test-dynamo: ## Run DynamoDB contract tests (moto, no AWS credentials needed)
	pytest tests/test_store_dynamo.py -v

test-all: test test-dynamo ## Run every test — Postgres + DynamoDB

clean: ## Remove caches
	rm -rf .pytest_cache .ruff_cache **/__pycache__ src/*.egg-info

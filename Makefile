CFG ?= examples/job.local.yaml

.PHONY: install test lint up down e2e e2e-torture e2e-multiregion traffic sdist

install:
	pip install -e ".[dev]"

test:
	pytest -q

lint:
	ruff check src tests

up:
	mkdir -p .local/extern && docker compose up -d cockroach spanner-emulator pubsub-emulator crdb-init

down:
	docker compose down -v

traffic:
	docker compose --profile traffic up -d movr-traffic

e2e:
	CFG=$(CFG) ./scripts/local_e2e.sh

e2e-torture:
	./scripts/e2e_torture.sh

e2e-multiregion:
	./scripts/e2e_multiregion.sh

sdist:
	python -m build --sdist

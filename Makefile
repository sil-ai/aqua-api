all: build-local

IMAGENAME ?=aqua-api-dev
REGISTRY ?= docker-local
GH_BRANCH=$(shell basename ${GITHUB_REF})

build-local:
	docker build  -t ${REGISTRY}/${IMAGENAME}:latest .

build-actions:
	docker build --force-rm=true -t ${REGISTRY}/${IMAGENAME}:latest .

# Boot the freshly built :latest image and verify it serves /health before we
# push it (issue #876). Catches image-only breakage — a missing COPY/ADD, a bad
# ENV PATH, an import error in the shipped layout — that the source-tree `make
# test` can't see because it runs against the host checkout, not the container.
# /health is a dependency-free liveness probe, so dummy AQUA_DB/SECRET_KEY (both
# required at import) are enough to prove the app actually booted.
smoke-test:
	docker rm -f aqua-smoke >/dev/null 2>&1 || true
	docker run -d --name aqua-smoke \
		-e AQUA_DB="postgresql+asyncpg://smoke:smoke@localhost:5432/smoke" \
		-e SECRET_KEY="smoke-test-key" \
		${REGISTRY}/${IMAGENAME}:latest \
		|| { echo "Smoke test FAILED: container did not start"; docker rm -f aqua-smoke >/dev/null 2>&1 || true; exit 1; }
	@ok=0; \
	for i in $$(seq 1 30); do \
		if docker exec aqua-smoke curl -fsS --connect-timeout 2 --max-time 5 http://localhost:8000/health; then echo " /health OK"; ok=1; break; fi; \
		sleep 2; \
	done; \
	docker logs aqua-smoke; \
	docker rm -f aqua-smoke >/dev/null 2>&1 || true; \
	if [ "$$ok" != "1" ]; then echo "Smoke test FAILED: /health did not return 200"; exit 1; fi

setup-pgvector:
	@echo "Setting up pgvector extension..."
	@docker exec -i $$(docker compose ps -q db) psql -U dbuser -d dbname -c "CREATE EXTENSION IF NOT EXISTS vector;" || echo "pgvector extension setup completed"
	@docker exec -i $$(docker compose ps -q db) psql -U dbuser -d dbname -c "SELECT * FROM pg_extension WHERE extname = 'vector';" || echo "pgvector verification completed"

localdb-up:
	@export AQUA_DB="postgresql://dbuser:dbpassword@localhost:5432/dbname" && \
	docker compose up -d db && \
	sleep 5 && \
	make setup-pgvector && \
	cd alembic && AQUA_DB="postgresql://dbuser:dbpassword@localhost:5432/dbname" alembic upgrade head && \
	cd ..


up:
	docker compose up -d

localapi-up:
	export PYTHONPATH=aqua-api && \
	docker compose up -d api

project-up:
	make down
	make localdb-up
	make localapi-up
	@export PYTHONPATH=${PWD} && \
	python test/conftest.py

prod-up:
	docker compose -f docker-compose.dev.yml up

down:
	docker compose down -v

test: linting localdb-up
	@export PYTHONPATH=${PWD} && \
	export AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" && \
	pytest test
	make down

# Rewrite the committed v3 OpenAPI contract baseline that
# test/test_openapi_contract.py guards. Run after an INTENTIONAL v3 change
# and commit the resulting snapshot diff (see issue #756, epic #842). No DB
# required — generating the schema does not touch the database.
regen-openapi-snapshot:
	@export PYTHONPATH=${PWD} && \
	python scripts/regen_openapi_snapshot.py


push-branch:
	docker push ${REGISTRY}/${IMAGENAME}:latest
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}
	docker push ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}

push-release:
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}
	docker push ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}


linting:
	@echo "Running linting"
	@black --check . --exclude '/(venv|\.venv|alembic)/'
	@echo "Black passed"
	@isort --check . --skip venv --skip .venv --skip ./alembic --profile black
	@echo "Isort passed"
	@flake8 . --exclude='venv,.venv,./alembic' --ignore=E501,W503,E203,E228,E226
	@echo "Linting passed"

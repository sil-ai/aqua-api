all: build-local

IMAGENAME ?=aqua-api-dev
REGISTRY ?= docker-local
GH_BRANCH=$(shell basename ${GITHUB_REF})

build-local:
	docker build  -t ${REGISTRY}/${IMAGENAME}:latest .

build-actions:
	docker build --force-rm=true -t ${REGISTRY}/${IMAGENAME}:latest .

localdb-up:
	@export AQUA_DB="postgresql://dbuser:dbpassword@localhost:5432/dbname" && \
	docker compose up -d db && \
	sleep 5 && \
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


push-branch:
	docker push ${REGISTRY}/${IMAGENAME}:latest
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}
	docker push ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}

push-release:
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}
	docker push ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}


linting:
	@echo "Running linting"
	@black --check . --exclude .venv
	@echo "Black passed"
	@isort --check . --skip .venv
	@echo "Isort passed"
	@flake8 . --exclude='**/v1/**,**/v2/**,./.venv,./alembic' --ignore=E501,W503,E203,E228,E226
	@echo "Linting passed"

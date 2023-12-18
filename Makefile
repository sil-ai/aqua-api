all: build-local

IMAGENAME=aqua-api
REGISTRY=docker-local
GH_BRANCH=$(shell basename ${GITHUB_REF})

build-local:
	docker build  -t ${REGISTRY}/${IMAGENAME}:latest .

build-actions:
	docker build --force-rm=true -t ${REGISTRY}/${IMAGENAME}:latest .

up:
	docker-compose up -d

down:
	docker-compose down

migrate:
	docker-compose run --rm api /bin/sh -c "cd /aws_database && alembic upgrade head "


test: migrate
	export AQUA_DB="postgresql://dbuser:dbpassword@db/dbname" && \
	docker-compose run --rm api pytest

	
push-branch:
	docker push ${REGISTRY}/${IMAGENAME}:latest
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}
	docker push ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}

push-release:
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}
	docker push ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}

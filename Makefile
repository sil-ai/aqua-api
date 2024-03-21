all: build-local

IMAGENAME=aqua-api-aws
REGISTRY ?= docker-local
GH_BRANCH=$(shell basename ${GITHUB_REF})

build-local:
	docker build  -t ${REGISTRY}/${IMAGENAME}:latest .

build-actions:
	docker build --force-rm=true -t ${REGISTRY}/${IMAGENAME}:latest .

localdb-up:
	@export AQUA_DB="postgresql://dbuser:dbpassword@localhost:5432/dbname" && \
	docker-compose up -d db && \
	sleep 5 && \
	cd alembic && AQUA_DB="postgresql://dbuser:dbpassword@localhost:5432/dbname" alembic upgrade head && \
	cd ..


up:
	docker-compose up -d

down:
	docker-compose down

test: localdb-up
	@export PYTHONPATH=${PWD} && \
	export AQUA_DB="postgresql://dbuser:dbpassword@localhost:5432/dbname" && \
	pytest test
	
push-branch:
	docker push ${REGISTRY}/${IMAGENAME}:latest
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}
	docker push ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}

push-release:
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}
	docker push ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}

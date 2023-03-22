all: build-local

IMAGENAME=aqua-api
GH_BRANCH=$(shell basename ${GITHUB_REF})

build-local:
	docker build -t ${REGISTRY}/${IMAGENAME}:latest .

build-actions:
	docker build --force-rm=true -t ${REGISTRY}/${IMAGENAME}:latest .

test:
	docker run --shm-size=1g postgres \
	-e AWS_ACCESS_KEY=${AWS_ACCESS_KEY} \
	-e AWS_SECRET_KEY=${AWS_SECRET_KEY} \
	-e GRAPHQL_URL=${GRAPHQL_URL} \
	-e GRAPHQL_SECRET=${GRAPHQL_SECRET} \
	-e AQUA_DB=${AQUA_DB} \
	-e TEST_KEY=${TEST_KEY} \
	-e FAIL_KEY=${FAIL_KEY} \
	-e KEY_VAULT=${KEY_VAULT} \
	-p 8000:8000 \
	${REGISTRY}/${IMAGENAME}:latest pytest

push-branch:
	docker push ${REGISTRY}/${IMAGENAME}:latest
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}
	docker push ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}

push-release:
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}
	docker push ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}

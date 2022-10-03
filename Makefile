all: build-local

IMAGENAME=aqua-api
API_REGISTRY=registry.digitalocean.com/aqua-tools
GH_BRANCH=$(shell basename ${GITHUB_REF})

build-local:
	docker build -t ${API_REGISTRY}/${IMAGENAME}:latest .

build-actions:
	docker build --force-rm=true -t ${API_REGISTRY}/${IMAGENAME}:latest .

test:
	docker run -e AWS_ACCESS_KEY=${AWS_ACCESS_KEY} \
	-e AWS_SECRET_KEY=${AWS_SECRET_KEY} \
	-e GRAPHQL_URL=${GRAPHQL_URL} \
	-e GRAPHQL_SECRET=${GRAPHQL_SECRET} \
	-e AQUA_DB=${AQUA_DB} \
	-e TEST_KEY=${TEST_KEY} \
	-e FAIL_KEY=${FAIL_KEY} \
	-e KEY_VAULT=${KEY_VAULT} \
	-p 8000:8000 \
	${API_REGISTRY}/${IMAGENAME}:latest pytest

push-branch:
	docker push ${API_REGISTRY}/${IMAGENAME}:latest
	docker tag ${API_REGISTRY}/${IMAGENAME}:latest ${API_REGISTRY}/${IMAGENAME}:${GITHUB_SHA}
	docker push ${API_REGISTRY}/${IMAGENAME}:${GITHUB_SHA}

push-release:
	docker tag ${API_REGISTRY}/${IMAGENAME}:latest ${API_REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}
	docker push ${API_REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}

build-pipelines:
	cd pipelines && ./build.sh

test-pipelines:
	cd pipelines && ./build.sh

push-pipelines:
	cd pipelines && ./push.sh

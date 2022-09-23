all: build-local

IMAGENAME=aqua-pipeline-pull-revision
GH_BRANCH=$(shell basename ${GITHUB_REF})

build-local:
	cp ../../../fixtures/vref.txt .
	docker build -t ${REGISTRY}/${IMAGENAME}:latest .
	rm vref.txt

build-actions:
	cp ../../../fixtures/vref.txt .
	docker build --force-rm=true -t ${REGISTRY}/${IMAGENAME}:latest .

test:
	docker run --rm -it -e AQUA_DB=${AQUA_DB} ${REGISTRY}/${IMAGENAME}:latest pytest

push-branch:
	docker push ${REGISTRY}/${IMAGENAME}:latest
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}
	docker push ${REGISTRY}/${IMAGENAME}:${GITHUB_SHA}

push-release:
	docker tag ${REGISTRY}/${IMAGENAME}:latest ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}
	docker push ${REGISTRY}/${IMAGENAME}:${RELEASE_VERSION}

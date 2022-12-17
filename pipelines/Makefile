IMAGENAME=aqua-pipeline-${PIPELINE}-pachyderm
TAG=$(shell git rev-parse --short HEAD)

build-docker-local:
	cp ../fixtures/vref.txt .
	docker build -t woodwardmw/${IMAGENAME}:latest -f scripts/${PIPELINE}/Dockerfile .
	rm vref.txt

push-docker:
	docker image tag woodwardmw/${IMAGENAME}:latest woodwardmw/${IMAGENAME}:${TAG}
	docker image push woodwardmw/${IMAGENAME}:${TAG}

docker-test-latest:
	TAG=$(curl -s -S "https://registry.hub.docker.com/v2/repositories/woodwardmw/${IMAGENAME}/tags/" | jq '."results"[]["name"]' | sed -n 1p | tr -d '"')
	docker pull woodwardmw/${IMAGENAME}:${TAG}
	docker run -it woodwardmw/${IMAGENAME}:${TAG} pytest

docker-build-push-test:
	make build-docker-local
	make push-docker
	make docker-test-latest

pull-local:
	minikube ssh docker pull woodwardmw/${IMAGENAME}:${TAG}

create-pipeline-pachyderm:
	pachctl create pipeline -f ../../create_cache.json 

update-pipelines-pachyderm:
	python specs/create_pipeline_json.py --tag ${TAG} --imagename ${IMAGENAME} --file specs/${PIPELINE}.json
	pachctl update pipeline -f specs/${PIPELINE}.json --reprocess

build-push-update:
	make build-docker-local
	make push-docker
	make update-pipelines-pachyderm

pachctl-put:
	pachctl put file ${REPO}@master:${notdir ${FILE}} -f ${FILE}

pachctl-logs:
	pachctl list jobs | awk -v row=2 -v column=1 'NR != row { next } { print $column }' | pachctl logs --job=word_alignment@$1

pachctl-get:
	pachctl get file -r top_source_scores@master:${LANGS} -o scripts/word_alignment/data/out/${LANGS}
	pachctl get file -r threshold_scores@master:/${LANGS}/threshold_scores.csv -o scripts/word_alignment/data/out/${LANGS}/threshold_scores.csv
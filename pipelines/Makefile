IMAGENAME=aqua-pipeline-${PIPELINE}-pachyderm
TAG=$(shell git rev-parse --short HEAD)

setup-pachyderm:
	minikube start --driver=kvm2
	kubectl get all  
	helm install --wait --timeout 40m pachd pach/pachyderm --set deployTarget=LOCAL  
	helm install --wait --timeout 40m pachd pach/pachyderm --set deployTarget=LOCAL
	kubectl get pods
	pachctl config import-kube local --overwrite
	pachctl config set active-context local
	pachctl port-forward

build-pachyderm-local:
	docker build -t woodwardmw/${IMAGENAME}:latest -f ${PIPELINE}/Dockerfile .

push-pachyderm:
	docker image tag woodwardmw/${IMAGENAME}:latest woodwardmw/${IMAGENAME}:${TAG}
	docker image push woodwardmw/${IMAGENAME}:${TAG}

pull-local:
	minikube ssh docker pull woodwardmw/${IMAGENAME}:${TAG}

create-repos-pachyderm:
	pachctl create repo sources
	pachctl create repo targets
	pachctl create repo target-cache
	pachctl create repo source-cache

create-pipeline-pachyderm:
	pachctl create pipeline -f ../../create_cache.json 

update-pipelines-pachyderm:
	python create_pipeline_json.py --tag ${TAG} --imagename ${IMAGENAME} --file ${PIPELINE}/${PIPELINE}.json
	pachctl update pipeline -f ${PIPELINE}/${PIPELINE}.json  --reprocess

build-push-update:
	make build-pachyderm-local
	make push-pachyderm
	make update-pipelines-pachyderm

pachctl-put:
	pachctl put file ${REPO}@master:${notdir ${FILE}} -f ${FILE}

pachctl-delete:
	pachctl delete file ${REPO}@master:${FILE}

pachctl-logs:
	pachctl list jobs | awk -v row=2 -v column=1 'NR != row { next } { print $column }' | pachctl logs --job=word_alignment@$1

pachctl-view-jobs:
	pachctl list jobs

pachctl-del-job:
	pachctl delete job word_alignment@${JOB}

pachctl-get:
	pachctl get file -r red_flags@master:${LANGS} -o scripts/word_alignment/data/out/${LANGS}
	pachctl get file -r word_alignment@master:${LANGS} -o scripts/word_alignment/data/out/${LANGS}
IMAGENAME=aqua-pipeline-word-alignment-pachyderm
TAG=36

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
	docker build -t woodwardmw/${IMAGENAME}:latest -f pachyderm/Dockerfile .

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
	pachctl update pipeline -f ../../word_alignment.json 
	pachctl update pipeline -f ../../create_source_cache.json 
	pachctl update pipeline -f ../../create_target_cache.json 


build-push-update:
	make -f Makefile.pachyderm build-pachyderm-local
	make -f Makefile.pachyderm push-pachyderm
	make -f Makefile.pachyderm update-pipelines-pachyderm

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
TAG=1.10.6

push: build
	docker push ingtranet/bitnami-airflow
	docker push ingtranet/bitnami-airflow-worker
	docker push ingtranet/bitnami-airflow-scheduler

build:
	docker build -t ingtranet/bitnami-airflow:${TAG}			--build-arg tag=${TAG} .
	docker build -t ingtranet/bitnami-airflow-worker:${TAG} 	--build-arg tag=${TAG} --build-arg postfix=-worker .
	docker build -t ingtranet/bitnami-airflow-scheduler:${TAG}	--build-arg tag=${TAG} --build-arg postfix=-scheduler .
	
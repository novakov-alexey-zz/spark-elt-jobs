VERSION=2.1.0

install:
	python3 -m venv airflow && source airflow/bin/activate
	python3 -m pip install -r requirements.txt
	
create-admin:
	airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

init-dags:
	airflow db init

docker-up:
	docker-compose -f docker/docker-compose.yaml up airflow-init
	docker-compose -f docker/docker-compose.yaml up
docker-down:
	docker-compose -f docker/docker-compose.yaml down airflow-init		
	docker-compose -f docker/docker-compose.yaml down
build-airflow-image:	
	cd github/airflow && git checkout tags/$(VERSION)
	docker build -t apache/airflow:custom-$(VERSION)  -f ./github/airflow/Dockerfile \
		--build-arg ADDITIONAL_PYTHON_DEPS="apache-airflow-providers-apache-spark dataclasses-json" \
		--build-arg AIRFLOW_VERSION=$(VERSION) \
		./github/airflow
	docker build -t apache/airflow:custom-jdk-$(VERSION) -f docker/Dockerfile ./docker	
docker-remove: 
	docker-compose -f docker/docker-compose.yaml down --volumes all
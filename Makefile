.ONESHELL:
.SHELL := /bin/bash

include ./private.env
export

AIRFLOW_VERSION=2.1.0

create-env:
	python3 -m venv airflow && source airflow/bin/activate
install:	
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
	cd github/airflow && git checkout tags/$(AIRFLOW_VERSION)
	docker build -t apache/airflow:custom-$(AIRFLOW_VERSION)  -f ./github/airflow/Dockerfile \
		--build-arg ADDITIONAL_PYTHON_DEPS="apache-airflow-providers-apache-spark dataclasses-json" \
		--build-arg AIRFLOW_VERSION=$(AIRFLOW_VERSION) \
		./github/airflow
	docker build -t apache/airflow:custom-jdk-$(AIRFLOW_VERSION) -f docker/Dockerfile ./docker	
docker-remove: 
	docker-compose -f docker/docker-compose.yaml down --volumes all

create-fn:
	aws lambda create-function \
	--function-name file-2-file \
	--role "arn:aws:iam::339364330848:role/Lambda2S3Bucket" \
	--code S3Bucket=lambda-code-jars-etl,S3Key=lambda-assembly-0.1.0-SNAPSHOT.jar \
	--runtime java11 \
	--memory 512 \
	--handler "etljobs.handler.FileToFileLambda::handleRequest"

update-fn: upload-fn
	aws lambda update-function-code \
	--function-name file-2-file \
	--s3-bucket lambda-code-jars-etl \
	--s3-key lambda-assembly-0.1.0-SNAPSHOT.jar

upload-fn:
	aws s3 cp ./modules/lambda/target/scala-2.12/lambda-assembly-0.1.0-SNAPSHOT.jar s3://lambda-code-jars-etl

delete-glue-job:
	aws cloudformation delete-stack \
        --stack-name glue-jobs
recreate-glue-job: delete-glue-job
	aws cloudformation create-stack \
    --stack-name glue-jobs \
    --template-body file://./cloud-formation/glue-jobs/file-to-file.yaml \
    --parameters ParameterKey=CFNIAMRoleName,ParameterValue=$(CFNIAMRoleName) ParameterKey=CFNExtraJars,ParameterValue=$(CFNExtraJars)
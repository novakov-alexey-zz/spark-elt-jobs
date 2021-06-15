install:
	python3 -m venv airflow && source airflow/bin/activate
	python3 -m pip install -r requirements.txt

set-dag-dir:
	export AIRFLOW__CORE__DAGS_FOLDER=./src	
	
create-admin:
	airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

init-dags:
	airflow db init
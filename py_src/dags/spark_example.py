from airflow import DAG
from airflow.models.connection import Connection
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from operators.file_operators import CheckReceivedFileOperator
from typing import Dict
import json

args = {
    'owner': 'alexey',
    'start_date': '2021-06-10',
    'provide_context': True
}


class ConnectionGrabber:
    def __getattr__(self, name):
        return Connection.get_connection_from_secrets(name)


def from_json(text: str) -> Dict:
    return json.loads(text)


dag = DAG(
    'spark_example',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    user_defined_macros={
        'connection': ConnectionGrabber(), 'fromjson': from_json},
    max_active_runs=1)


file_to_location_job = SparkSubmitOperator(
    task_id='file2location',
    conn_id='spark_default',
    java_class='example.FileToFile',
    application='local:///Users/Alexey_Novakov/dev/git/airflow-poc/target/scala-2.12/spark-jobs_2.12-0.1.0-SNAPSHOT.jar',
    application_args=["-i",
                      "{{fromjson(connection.fs_local_input.extra)['path']}}",
                      "-o",
                      "{{fromjson(connection.fs_local_raw_data.extra)['path']}}",
                      "--execution-date",
                      "{{ds}}",
                      "-d",
                      "{{dag.dag_id}}",
                      "-t",
                      "{{task.task_id}}",
                      "--glob-pattern",
                      "items_{{ ds }}.csv"],
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    name='airflow-file-2-file',
    verbose=True,
    driver_memory='1g',
    dag=dag,
)

check_file = CheckReceivedFileOperator(
    task_id='check_file',
    file_mask="*_{{ ds }}.csv",
    file_prefixes={"items", "orders", "customers"},
    dst_conn_id='fs_local_raw_data',
    dag=dag)

file_to_location_job >> check_file

if __name__ == "__main__":
    dag.cli()

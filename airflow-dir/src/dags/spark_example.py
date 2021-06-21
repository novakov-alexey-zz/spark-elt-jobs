from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from operators.file_operators import CheckReceivedFileOperator
from dags.macros import ConnectionGrabber, from_json
from typing import List, Tuple
from functools import reduce

args = {
    'owner': 'alexey',
    'start_date': '2021-06-10',
    'provide_context': True
}

dag = DAG(
    'spark_example',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    user_defined_macros={
        'connection': ConnectionGrabber(), 'fromjson': from_json},
    max_active_runs=1)

file_prefixes = {"items", "orders", "customers"}


def etl_job_args(input_path: str, output_path: str, entity_patterns: List[Tuple[str, str]]) -> List[str]:
    args = ["-i",
            input_path,
            "-o",
            output_path,
            "--execution-date",
            "{{ds}}",
            "-d",
            "{{dag.dag_id}}",
            "--overwrite"
            ]

    for (entity_name, prefix) in entity_patterns:
        args = args + ["--glob-pattern", entity_name +
                       ":" + prefix + "_{{ ds }}.csv"]

    return args


def spark_copy(task_id: str, entity_patterns: List[Tuple[str, str]]) -> BaseOperator:
    formats_args = ["--input-format", "csv", "--output-format", "parquet"]
    input_path = "{{fromjson(connection.fs_local_raw_data.extra)['path']}}"
    output_path = "{{fromjson(connection.fs_local_dw.extra)['path']}}"
    args = formats_args + \
        etl_job_args(input_path, output_path, entity_patterns)

    return SparkSubmitOperator(
        task_id=task_id,
        conn_id='spark_default',
        java_class='etljobs.spark.FileToDataset',
        application="{{fromjson(connection.etl_jobs_spark_jar.extra)['path']}}",
        application_args=args,
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        name='airflow-file-2-file',
        verbose=True,
        driver_memory='1g',
        dag=dag
    )


def mkString(l: List[str], sep: str = ' ') -> str:
    return reduce(lambda a, b: a + ' ' + b, l)


def hadoop_copy(task_id: str, entity_patterns: List[Tuple[str, str]]) -> BaseOperator:
    input_path = "{{fromjson(connection.fs_local_input.extra)['path']}}"
    output_path = "{{fromjson(connection.fs_local_raw_data.extra)['path']}}"
    args_list = etl_job_args(input_path, output_path, entity_patterns)
    args_list = args_list + \
        ["--processed-dir",
            "{{fromjson(connection.fs_local_input.extra)['path']}}/processed"]
    args = mkString(args_list)
    return BashOperator(
        task_id=task_id,
        bash_command="java -cp {{fromjson(connection.etl_jobs_hadoop_jar.extra)['path']}} etljobs.hadoop.FileToFile " + args,
        skip_exit_code=None,
        dag=dag
    )


extract_file_task = hadoop_copy(f'file2location_all', [('all', '*')])
file_prefixes_args = ["--file-prefixes " + p for p in file_prefixes]

check_file_args_list = [
    "-i",
    "{{fromjson(connection.fs_local_raw_data.extra)['path']}}",
    "--execution-date",
    "{{ds}}",
    "-d",
    "{{dag.dag_id}}",
    "--glob-pattern",
    "*_{{ ds }}.csv"
] + file_prefixes_args

check_file_args = mkString(check_file_args_list)

check_files = BashOperator(
    task_id='check_file',
    bash_command="java -cp {{fromjson(connection.etl_jobs_hadoop_jar.extra)['path']}} etljobs.hadoop.CheckFileExists " + check_file_args,
    skip_exit_code=99,
    dag=dag
)

files_to_dataset = spark_copy(
    f'file2dataset', map(lambda a: (a, a), file_prefixes))

extract_file_task >> check_files >> files_to_dataset

if __name__ == "__main__":
    dag.cli()

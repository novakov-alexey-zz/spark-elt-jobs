from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from dags.macros import ConnectionGrabber, from_json
from typing import List, Tuple, Set
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

LOCAL_INPUT = "{{fromjson(connection.fs_local_input.extra)['path']}}"
LOCAL_RAW_DATA = "{{fromjson(connection.fs_local_raw_data.extra)['path']}}"
LOCAL_DATAWAREHOUSE = "{{fromjson(connection.fs_local_dw.extra)['path']}}"
SPARK_JOBS_JAR = "{{fromjson(connection.etl_jobs_spark_jar.extra)['path']}}"
HADOOP_JOBS_JAR = "{{fromjson(connection.etl_jobs_hadoop_jar.extra)['path']}}"
INPUT_SCHEMA = "{{fromjson(connection.input_schemas.extra)['path']}}"


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
                       ":" + prefix + "_*{{ ds }}.csv"]

    return args


def spark_batch_job(task_id: str, entity_patterns: List[Tuple[str, str]]) -> BaseOperator:
    return spark_copy(task_id, entity_patterns, 'etljobs.spark.FileToDataset')


def spark_stream_job(task_id: str, entity_patterns: List[Tuple[str, str]]) -> BaseOperator:
    return spark_copy(task_id, entity_patterns, 'etljobs.spark.FileStreamToDataset')


def spark_copy(task_id: str, entity_patterns: List[Tuple[str, str]], main_class: str) -> BaseOperator:
    formats_args = ["--input-format", "csv", "--output-format", "parquet"]
    input_schema_path = ["-s", INPUT_SCHEMA + "/" + "{{dag.dag_id}}"]
    args = formats_args + \
        etl_job_args(LOCAL_RAW_DATA, LOCAL_DATAWAREHOUSE, entity_patterns) + \
        ["--move-files", "--reader-options", "header:true"] + input_schema_path

    return SparkSubmitOperator(
        task_id=task_id,
        conn_id='spark_default',
        java_class=main_class,
        application=SPARK_JOBS_JAR,
        application_args=args,
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        name=task_id,
        verbose=True,
        driver_memory='1g',
        dag=dag
    )


def mkString(l: List[str], sep: str = ' ') -> str:
    return reduce(lambda a, b: a + ' ' + b, l)


def hadoop_copy(task_id: str, entity_patterns: List[Tuple[str, str]]) -> BaseOperator:
    output_path = LOCAL_RAW_DATA
    processed_dir = ["--processed-dir", LOCAL_INPUT + "/processed"]
    args_list = etl_job_args(LOCAL_INPUT, output_path,
                             entity_patterns) + processed_dir
    args = mkString(args_list)

    return BashOperator(
        task_id=task_id,
        bash_command="java -cp " + HADOOP_JOBS_JAR +
        " etljobs.hadoop.FileToFile " + args,
        skip_exit_code=None,
        dag=dag
    )


def check_files_task(file_prefixes: Set[str]) -> BaseOperator:
    file_prefixes_args = ["--file-prefixes " + p for p in file_prefixes]

    check_file_args_list = [
        "-i",
        LOCAL_RAW_DATA,
        "--execution-date",
        "{{ds}}",
        "-d",
        "{{dag.dag_id}}",
        "--glob-pattern",
        "*_{{ ds }}.csv"
    ] + file_prefixes_args

    check_file_args = mkString(check_file_args_list)

    return BashOperator(
        task_id='check-file',
        bash_command="java -cp "+HADOOP_JOBS_JAR +
        " etljobs.hadoop.CheckFileExists " + check_file_args,
        skip_exit_code=99,
        dag=dag
    )


file_prefixes = {"items", "orders", "customers"}
extract_file_task = hadoop_copy(f'file-2-location', [('all', '*')])
check_files = check_files_task(file_prefixes)
entity_patterns = map(lambda a: (a, a), file_prefixes)
# files_to_dataset = spark_batch_job(
#     f'file-2-dataset', entity_patterns)
files_stream_to_dataset = spark_stream_job(
    f'file-stream-2-dataset', entity_patterns)

extract_file_task >> check_files >> files_stream_to_dataset

if __name__ == "__main__":
    dag.cli()

from dataclasses import dataclass
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta
from dags.macros import ConnectionGrabber, from_json
from typing import List, Tuple, Set, Optional
from functools import reduce


@dataclass
class EntityPattern:
    name: str
    pattern: str
    dedupKey: Optional[str] = None


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

LOCAL_INPUT = "{{fromjson(connection.s3_local.extra)['inputPath']}}"
LOCAL_RAW_DATA = "{{fromjson(connection.s3_local.extra)['rawPath']}}"
LOCAL_DATAWAREHOUSE = "{{fromjson(connection.s3_local.extra)['dwPath']}}"

SPARK_JOBS_JAR = "{{fromjson(connection.etl_jobs_spark_jar.extra)['path']}}"
HADOOP_JOBS_JAR = "{{fromjson(connection.etl_jobs_hadoop_jar.extra)['path']}}"
INPUT_SCHEMA = "{{fromjson(connection.input_schemas.extra)['path']}}"

s3_conn = BaseHook.get_connection("s3_local")


def hadoop_options():
    return [
        ("fs.s3a.connection.ssl.enabled",
         "{{fromjson(connection.s3_local.extra)['fs.s3a.ssl']}}"),
        ("fs.s3a.endpoint",
         "{{fromjson(connection.s3_local.extra)['fs.s3a.endpoint']}}"),
        ("fs.s3a.access.key", "minioadmin"),
        ("fs.s3a.secret.key", "minioadmin")
    ]


def common_args(input_path: str, output_path: str, entity_patterns: List[EntityPattern]) -> List[str]:
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

    for e in entity_patterns:
        dedupKey = ("" if e.dedupKey is None else ":" + e.dedupKey)
        pattern = e.name + ":" + e.pattern + "_*{{ ds }}.csv"
        args = args + ["--entity-pattern", pattern + dedupKey]

    return args


def spark_batch_job(task_id: str, entity_patterns: List[EntityPattern]) -> BaseOperator:
    return spark_copy(task_id, entity_patterns, 'etljobs.spark.FileToDataset')


def spark_stream_job(task_id: str, entity_patterns: List[EntityPattern]) -> BaseOperator:
    return spark_copy(task_id, entity_patterns, 'etljobs.spark.FileStreamToDataset')


def hadoop_cfg(options: List[Tuple[str, str]], arg_prefix: str = "") -> List[str]:
    args = []
    for o in options:
        args.append("--hadoop-config")
        args.append(arg_prefix + o[0] + ":" + o[1])
    return args


def spark_copy(task_id: str, entity_patterns: List[EntityPattern], main_class: str) -> BaseOperator:
    input_schema_path = ["-s", INPUT_SCHEMA + "/" + "{{dag.dag_id}}"]
    formats = ["--input-format", "csv", "--output-format", "delta"]
    partitioning = ["--partition-by", "date"]
    reader_options = ["--reader-options", "header:true"]
    spark_hadoop_options = hadoop_cfg(hadoop_options(), "spark.hadoop.")

    common = common_args(LOCAL_RAW_DATA, LOCAL_DATAWAREHOUSE, entity_patterns)
    copy_args = formats + common + \
        ["--move-files"] + input_schema_path + \
        reader_options + partitioning + spark_hadoop_options

    return SparkSubmitOperator(
        task_id=task_id,
        conn_id='spark_default',
        java_class=main_class,
        application=SPARK_JOBS_JAR,
        application_args=copy_args,
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        name=task_id,
        verbose=False,
        driver_memory='1g',
        dag=dag
    )


def mkString(l: List[str], sep: str = ' ') -> str:
    return reduce(lambda a, b: f"{a}{sep}{b}", l)


def hadoop_copy(task_id: str, entity_patterns: List[EntityPattern]) -> BaseOperator:
    output_path = LOCAL_RAW_DATA
    processed_dir = ["--processed-dir", LOCAL_INPUT + "/processed"]
    hadoop_opts = hadoop_cfg(hadoop_options())
    args_list = common_args(LOCAL_INPUT, output_path,
                            entity_patterns) + processed_dir + hadoop_opts
    args = mkString(args_list)

    return BashOperator(
        task_id=task_id,
        bash_command="java -cp " + HADOOP_JOBS_JAR +
        " etljobs.hadoop.FileToFile " + args,
        skip_exit_code=None,
        dag=dag
    )


def check_files_task(entity_patterns: List[EntityPattern]) -> BaseOperator:
    file_prefixes_args = ["--file-prefixes " +
                          p.pattern for p in entity_patterns]

    hadoop_opts = hadoop_cfg(hadoop_options())
    check_file_args_list = [
        "-i",
        LOCAL_RAW_DATA,
        "--execution-date",
        "{{ds}}",
        "-d",
        "{{dag.dag_id}}",
        "--glob-pattern",
        "*_{{ ds }}.csv"
    ] + file_prefixes_args + hadoop_opts

    check_file_args = mkString(check_file_args_list)

    return BashOperator(
        task_id='check-file',
        bash_command="java -cp " + HADOOP_JOBS_JAR +
        " etljobs.hadoop.CheckFileExists " + check_file_args,
        skip_exit_code=99,
        dag=dag
    )


entity_patterns = [
    EntityPattern("items", "items", "itemId"),
    EntityPattern("orders", "orders", "orderId"),
    EntityPattern("customers", "customers", "customerId")
]

extract_file_task = hadoop_copy('file-2-location', [EntityPattern('all', '*')])
check_files = check_files_task(entity_patterns)
# files_to_dataset = spark_batch_job(
# 'file-2-dataset', entity_patterns)
files_stream_to_dataset = spark_stream_job(
    'file-stream-2-dataset', entity_patterns)

extract_file_task >> check_files >> files_stream_to_dataset

if __name__ == "__main__":
    dag.cli()

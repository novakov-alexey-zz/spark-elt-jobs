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


@dataclass
class HadoopJobCfg:
    input_path: str
    output_path: str
    hadoop_options: List[Tuple[str, str]]
    processed_path: str
    entity_patterns: List[EntityPattern]
    execution_date: str = "{{ds}}"
    dag_id: str = "{{dag.dag_id}}"

    def to_arg_list(self) -> List[str]:
        args = [
            "-i",
            self.input_path,
            "--execution-date",
            self.execution_date,
            "-d",
            self.dag_id,
            "-o",
            self.output_path,
            "--processed-dir",
            self.processed_path
        ]

        args += hadoop_options_to_args(self.hadoop_options)
        args += entity_patterns_to_args(self.entity_patterns)

        return args


@dataclass
class CheckFileCfg:
    hadoop_options: List[Tuple[str, str]]
    file_prefixes: List[str]
    input_path: str
    glob_pattern: str
    execution_date: str = "{{ds}}"
    dag_id: str = "{{dag.dag_id}}"

    def to_arg_list(self) -> List[str]:
        args = [
            "-i",
            self.input_path,
            "--execution-date",
            self.execution_date,
            "-d",
            self.dag_id,
            "--glob-pattern",
            self.glob_pattern
        ]
        
        for p in self.file_prefixes:
            args.append("--file-prefixes")
            args.append(p)

        args += hadoop_options_to_args(self.hadoop_options)

        return args


@dataclass
class SparkJobCfg:
    input_path: str
    output_path: str
    reader_options: List[str]
    hadoop_options: List[Tuple[str, str]]
    partition_by: str
    entity_patterns: List[EntityPattern]
    input_schema_path: str
    input_format: str = "csv"
    output_format: str = "delta"
    execution_date: str = "{{ds}}"
    dag_id: str = "{{dag.dag_id}}"
    overwrite: bool = True
    move_files: bool = True
    hadoop_options_prefix: Optional[str] = "spark.hadoop."

    def to_arg_list(self) -> List[str]:
        args = ["-i",
                self.input_path,
                "-o",
                self.output_path,
                "--execution-date",
                self.execution_date,
                "-d",
                self.dag_id
                ]

        if self.overwrite:
            args.append("--overwrite")

        if self.move_files:
            args.append("--move-files")

        args += ["-s", self.input_schema_path]

        formats = ["--input-format", self.input_format,
                   "--output-format", self.output_format]
        args += formats

        for o in self.reader_options:
            args += ["--reader-options", o]

        args += hadoop_options_to_args(
            self.hadoop_options, self.hadoop_options_prefix)

        args += ["--partition-by", self.partition_by]

        args += entity_patterns_to_args(self.entity_patterns)

        return args


def entity_patterns_to_args(entity_patterns: List[EntityPattern]) -> List[str]:
    args = []
    for e in entity_patterns:
        dedupKey = ("" if e.dedupKey is None else ":" + e.dedupKey)
        pattern = e.name + ":" + e.pattern + "_*{{ ds }}.csv"
        args += ["--entity-pattern", pattern + dedupKey]

    return args


def hadoop_options_to_args(options: List[Tuple[str, str]], prefix: Optional[str] = None) -> List[str]:
    args = []
    for name, value in options:
        args.append("--hadoop-config")
        args.append((prefix if prefix is not None else "") +
                    name + ":" + value)
    return args


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


def hadoop_options() -> List[Tuple[str, str]]:
    return [
        ("fs.s3a.connection.ssl.enabled",
         "{{fromjson(connection.s3_local.extra)['fs.s3a.ssl']}}"),
        ("fs.s3a.endpoint",
         "{{fromjson(connection.s3_local.extra)['fs.s3a.endpoint']}}"),
        ("fs.s3a.access.key", s3_conn.login),
        ("fs.s3a.secret.key", s3_conn.password)
    ]


def spark_batch_job(task_id: str, cfg: SparkJobCfg) -> BaseOperator:
    return spark_copy(task_id, cfg, 'etljobs.spark.FileToDataset')


def spark_stream_job(task_id: str, cfg: SparkJobCfg) -> BaseOperator:
    return spark_copy(task_id, cfg, 'etljobs.spark.FileStreamToDataset')


def spark_copy(task_id: str, cfg: SparkJobCfg, main_class: str) -> BaseOperator:
    copy_args = cfg.to_arg_list()

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


def hadoop_copy(task_id: str, cfg: HadoopJobCfg) -> BaseOperator:
    return BashOperator(
        task_id=task_id,
        bash_command="java -cp " + HADOOP_JOBS_JAR +
        " etljobs.hadoop.FileToFile " + mkString(cfg.to_arg_list()),
        skip_exit_code=None,
        dag=dag
    )


def check_files_task(cfg: CheckFileCfg) -> BaseOperator:
    return BashOperator(
        task_id='check-file',
        bash_command="java -cp " + HADOOP_JOBS_JAR +
        " etljobs.hadoop.CheckFileExists " + mkString(cfg.to_arg_list()),
        skip_exit_code=99,
        dag=dag
    )

########################################
# DAG construction site
########################################


entity_patterns = [
    EntityPattern("items", "items", "itemId"),
    EntityPattern("orders", "orders", "orderId"),
    EntityPattern("customers", "customers", "customerId")
]

extract_files_cfg = HadoopJobCfg(
    input_path=LOCAL_INPUT,
    output_path=LOCAL_RAW_DATA,
    processed_path=LOCAL_INPUT + "/processed",
    hadoop_options=hadoop_options(),
    entity_patterns=[EntityPattern('all', '*')]
)
extract_file_task = hadoop_copy('file-2-location', extract_files_cfg)

check_file_cfg = CheckFileCfg(
    glob_pattern="*_{{ ds }}.csv",
    file_prefixes=[p.pattern for p in entity_patterns],
    input_path=LOCAL_RAW_DATA,
    hadoop_options=hadoop_options()
)
check_files = check_files_task(check_file_cfg)


# files_to_dataset = spark_batch_job(
# 'file-2-dataset', spark_job_cfg)
spark_job_cfg = SparkJobCfg(
    input_path=LOCAL_RAW_DATA,
    output_path=LOCAL_DATAWAREHOUSE,
    entity_patterns=entity_patterns,
    reader_options=["header:true"],
    hadoop_options=hadoop_options(),
    partition_by="date",
    input_schema_path=INPUT_SCHEMA + "/" + "{{dag.dag_id}}"
)
files_stream_to_dataset = spark_stream_job(
    'file-stream-2-dataset', spark_job_cfg)

extract_file_task >> check_files >> files_stream_to_dataset

if __name__ == "__main__":
    dag.cli()

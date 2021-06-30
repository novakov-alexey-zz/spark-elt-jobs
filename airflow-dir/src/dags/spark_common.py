from dataclasses import dataclass
from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from typing import List, Tuple, Set, Optional
from dags.macros import ConnectionGrabber, from_json
from airflow.hooks.base import BaseHook


@dataclass
class EntityPattern:
    name: str
    pattern: str
    dedupKey: Optional[str] = None


class ArgList:
    def to_arg_list(self) -> List[str]:
        pass

@dataclass
class SparkJobCfg(ArgList):
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


def spark_stream_job(task_id: str, cfg: ArgList, dag: DAG) -> BaseOperator:
    return spark_job(task_id, cfg, 'etljobs.spark.FileStreamToDataset', dag)


def spark_job(task_id: str, cfg: ArgList, main_class: str, dag: DAG) -> BaseOperator:
    copy_args = cfg.to_arg_list()

    return SparkSubmitOperator(
        task_id=task_id,
        conn_id='spark_default',
        java_class=main_class,
        application=SPARK_JOBS_JAR,
        application_args=copy_args,
        total_executor_cores='2',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        name=task_id,
        verbose=False,
        driver_memory='1g',        
        dag=dag
    )


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


LOCAL_INPUT = "{{fromjson(connection.s3_local.extra)['inputPath']}}"
LOCAL_DATAWAREHOUSE = "{{fromjson(connection.s3_local.extra)['dwPath']}}"

SPARK_JOBS_JAR = "{{fromjson(connection.etl_jobs_spark_jar.extra)['path']}}"
INPUT_SCHEMA = "{{fromjson(connection.input_schemas.extra)['path']}}"
dag_schema_path = INPUT_SCHEMA + "/spark_example"

entity_patterns = [
    EntityPattern("items", "items", "itemId"),
    EntityPattern("orders", "orders", "orderId"),
    EntityPattern("customers", "customers", "customerId")
]

user_defined_macros = {
    'connection': ConnectionGrabber(), 'fromjson': from_json}

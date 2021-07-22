from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from dags.macros import ConnectionGrabber, from_json
from dataclasses import dataclass
from operators.spark import SparkSubmitReturnCode
from typing import List, Tuple, Optional, Generator

from airflow import DAG


@dataclass
class EntityPattern:
    name: str
    pattern: str
    dedup_key: Optional[str] = None


class ArgList:
    def to_arg_list(self) -> List[str]:
        pass


@dataclass
class SparkJobCfg(ArgList):
    input_path: str
    output_path: str
    reader_options: List[str]
    hadoop_options: List[Tuple[str, str]]
    partition_by: List[str]
    entity_patterns: List[EntityPattern]
    input_schema_path: str
    trigger_interval: int = -1
    input_format: str = "csv"
    output_format: str = "delta"
    execution_date: str = "{{ds}}"
    job_id: str = "{{dag.dag_id}}"
    overwrite: bool = True
    move_files: bool = True
    stream_move_files: bool = False
    hadoop_options_prefix: Optional[str] = "spark.hadoop."
    hudi_sync_to_hive: bool = False

    def to_arg_list(self) -> List[str]:
        args = ["-i",
                self.input_path,
                "-o",
                self.output_path,
                "--execution-date",
                self.execution_date,
                "-j",
                self.job_id
                ]

        if self.overwrite:
            args.append("--overwrite")

        if self.move_files:
            args.append("--move-files")

        if self.stream_move_files:
            args.append("--stream-move-files")

        args += ["-s", self.input_schema_path]
        args += ["--trigger-interval", str(self.trigger_interval)]

        formats = ["--input-format", self.input_format,
                   "--output-format", self.output_format]
        args += formats

        for o in self.reader_options:
            args += ["--reader-options", o]

        args += hadoop_options_to_args(
            self.hadoop_options, self.hadoop_options_prefix)

        for p in self.partition_by:
            args += ["--partition-by", p]

        args += entity_patterns_to_args(self.entity_patterns)

        if self.hudi_sync_to_hive:
            args += ["--hudi-sync-to-hive"]

        return args


def entity_patterns_to_args(patterns: List[EntityPattern]) -> Generator[str, None, None]:
    for e in patterns:
        dedup_key = ("" if e.dedup_key is None else ":" + e.dedup_key)
        pattern = e.name + ":" + e.pattern + "_*{{ ds }}.csv"
        yield "--entity-pattern"
        yield pattern + dedup_key


def hadoop_options_to_args(options: List[Tuple[str, str]], prefix: Optional[str] = None) -> Generator[str, None, None]:
    for name, value in options:
        yield "--hadoop-config"
        yield (prefix if prefix is not None else "") + name + ":" + value


def spark_stream_job(task_id: str, cfg: ArgList, dag: DAG, skip_exit_code: Optional[int] = None,
                     track_driver: bool = True) -> BaseOperator:
    return spark_job(task_id, cfg, 'etljobs.spark.FileStreamToDataset', dag, skip_exit_code, track_driver)


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


def spark_job(task_id: str, cfg: ArgList, main_class: str, dag: DAG, skip_exit_code: Optional[int] = None,
              track_driver: bool = True, application: str = SPARK_JOBS_JAR) -> BaseOperator:
    job_args = cfg.to_arg_list()

    return SparkSubmitReturnCode(
        skip_exit_code=skip_exit_code,
        track_driver=track_driver,
        task_id=task_id,
        conn_id='spark_default',
        java_class=main_class,
        application=application,
        application_args=job_args,
        total_executor_cores='2',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        name=task_id,
        verbose=False,
        driver_memory='1g',
        dag=dag,
        do_xcom_push=True
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

from dataclasses import dataclass
from datetime import timedelta
from functools import reduce
from typing import List, Tuple

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from dags.spark_common import EntityPattern, SparkJobCfg, entity_patterns_to_args, dag_schema_path, spark_job
from dags.spark_common import hadoop_options_to_args, spark_stream_job, entity_patterns, hadoop_options, \
    user_defined_macros, LOCAL_INPUT, LOCAL_DATAWAREHOUSE


@dataclass
class HadoopJobCfg:
    input_path: str
    output_path: str
    hadoop_options: List[Tuple[str, str]]
    processed_path: str
    entity_patterns: List[EntityPattern]
    execution_date: str = "{{ds}}"
    job_id: str = "{{dag.dag_id}}"

    def to_arg_list(self) -> List[str]:
        args = [
            "-i",
            self.input_path,
            "--execution-date",
            self.execution_date,
            "-j",
            self.job_id,
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
    job_id: str = "{{dag.dag_id}}"

    def to_arg_list(self) -> List[str]:
        args = [
            "-i",
            self.input_path,
            "--execution-date",
            self.execution_date,
            "-j",
            self.job_id,
            "--glob-pattern",
            self.glob_pattern
        ]

        for p in self.file_prefixes:
            args.append("--file-prefixes")
            args.append(p)

        args += hadoop_options_to_args(self.hadoop_options)

        return args


LOCAL_RAW_DATA = "{{fromjson(connection.s3_local.extra)['rawPath']}}"
HADOOP_JOBS_JAR = "{{fromjson(connection.etl_jobs_hadoop_jar.extra)['path']}}"


def spark_batch_job(task_id: str, cfg: SparkJobCfg, dag: DAG) -> BaseOperator:
    return spark_job(task_id, cfg, 'etljobs.spark.FileToDataset', dag)


def mkString(l: List[str], sep: str = ' ') -> str:
    return reduce(lambda a, b: f"{a}{sep}{b}", l)


def hadoop_copy(task_id: str, cfg: HadoopJobCfg, dag: DAG) -> BaseOperator:
    return BashOperator(
        task_id=task_id,
        bash_command="java -cp " + HADOOP_JOBS_JAR +
                     " etljobs.hadoop.FileToFile " + mkString(cfg.to_arg_list()),
        dag=dag
    )


def check_files_task(cfg: CheckFileCfg, dag: DAG) -> BaseOperator:
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


args = {
    'owner': 'alexey',
    'start_date': '2021-06-10',
    'provide_context': True
}

dag = DAG(
    'spark_example_files',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    user_defined_macros=user_defined_macros,
    max_active_runs=1)

hadoop_job_cfg = HadoopJobCfg(
    input_path=LOCAL_INPUT,
    output_path=LOCAL_RAW_DATA,
    processed_path=LOCAL_INPUT + "/processed",
    hadoop_options=hadoop_options(),
    entity_patterns=[EntityPattern('all', '*')]
)
extract_file_task = hadoop_copy('file-2-location', hadoop_job_cfg, dag)

check_file_cfg = CheckFileCfg(
    glob_pattern="*_{{ ds }}.csv",
    file_prefixes=[p.pattern for p in entity_patterns],
    input_path=LOCAL_RAW_DATA,
    hadoop_options=hadoop_options()
)
check_files = check_files_task(check_file_cfg, dag)

# files_to_dataset = spark_batch_job(
# 'file-2-dataset', spark_job_cfg)
spark_job_cfg = SparkJobCfg(
    input_path=LOCAL_RAW_DATA,
    output_path=LOCAL_DATAWAREHOUSE,
    entity_patterns=entity_patterns,
    reader_options=["header:true"],
    hadoop_options=hadoop_options(),
    partition_by="date",
    input_schema_path=dag_schema_path,
)
files_stream_to_dataset = spark_stream_job(
    'file-stream-2-dataset', spark_job_cfg, dag)

extract_file_task >> check_files >> files_stream_to_dataset

if __name__ == "__main__":
    dag.cli()

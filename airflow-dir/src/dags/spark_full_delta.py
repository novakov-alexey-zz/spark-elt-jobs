from dataclasses import dataclass
from airflow import DAG
from datetime import timedelta
from typing import List, Tuple, Optional

from dags.spark_common import hadoop_options_to_args, dag_schema_path, spark_job, hadoop_options, LOCAL_INPUT, LOCAL_DATAWAREHOUSE
from dags.spark_common import SparkJobCfg, spark_stream_job, entity_patterns, user_defined_macros, ArgList


@dataclass
class CheckDataCfg(ArgList):
    input_path: str
    hadoop_options: List[Tuple[str, str]]
    entities: List[str]
    input_format: str
    execution_date: str = "{{ds}}"
    dag_id: str = "{{dag.dag_id}}"
    date_column: str = "date"
    hadoop_options_prefix: Optional[str] = "spark.hadoop."

    def to_arg_list(self) -> List[str]:
        args = ["-i",
                self.input_path,
                "--execution-date",
                self.execution_date,
                "-d",
                self.dag_id,
                "--date-column",
                self.date_column,
                "--input-format",
                self.input_format
                ]

        args += hadoop_options_to_args(
            self.hadoop_options, self.hadoop_options_prefix)

        for e in self.entities:
            args += ["--entities", e]

        return args

########################################
# DAG construction site
########################################


args = {
    'owner': 'alexey',
    'start_date': '2021-06-10'
}

dag = DAG(
    'spark_full_delta',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    user_defined_macros=user_defined_macros,
    max_active_runs=1)

spark_streaming_job_cfg = SparkJobCfg(
    input_path=LOCAL_INPUT,
    output_path=LOCAL_DATAWAREHOUSE,
    entity_patterns=entity_patterns,
    reader_options=["header:true"],
    hadoop_options=hadoop_options(),
    partition_by="date",
    input_schema_path=dag_schema_path
)
extract_file_task = spark_stream_job(
    'file-2-location', spark_streaming_job_cfg, dag)

check_data_task = spark_job('check-data', CheckDataCfg(
    input_path=LOCAL_DATAWAREHOUSE,
    hadoop_options=hadoop_options(),
    entities=[e.name for e in entity_patterns],
    input_format="delta"
), 'etljobs.spark.CheckDataRecieved', dag)

extract_file_task >> check_data_task

if __name__ == "__main__":
    dag.cli()

from dags.spark_common import SparkJobCfg, spark_stream_job, entity_patterns, user_defined_macros
from dags.spark_common import dag_schema_path, hadoop_options, LOCAL_INPUT, LOCAL_DATAWAREHOUSE
from datetime import timedelta

from airflow import DAG

args = {
    'owner': 'alexey',
    'start_date': '2021-06-10'
}

dag = DAG(
    'spark_streaming',
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
    partition_by=["year", "month", "day"],
    input_schema_path=dag_schema_path,
    trigger_interval=3000
)
extract_file_task = spark_stream_job(
    'file_2_location', spark_streaming_job_cfg, dag, track_driver=True)

extract_file_task

if __name__ == "__main__":
    dag.cli()

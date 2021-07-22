from dags.spark_common import SparkJobCfg, spark_job, user_defined_macros, EntityPattern
from dags.spark_common import dag_schema_path, hadoop_options, LOCAL_INPUT, LOCAL_DATAWAREHOUSE
from datetime import timedelta

from airflow import DAG

args = {
    'owner': 'alexey',
    'start_date': '2021-06-10'
}

dag = DAG(
    'spark_hudi',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    user_defined_macros=user_defined_macros,
    max_active_runs=1)

entity_patterns = [
    EntityPattern("orders", "orders", "orderId"),
]

cfg = SparkJobCfg(
    input_path=LOCAL_INPUT,
    output_path=LOCAL_DATAWAREHOUSE,
    entity_patterns=entity_patterns,
    reader_options=["header:true"],
    hadoop_options=hadoop_options(),
    partition_by=["year", "month", "day"],
    input_schema_path=dag_schema_path,
    output_format="hudi",
    trigger_interval=5000
)
JAR_PATH = "{{fromjson(connection.etl_jobs_emr_jar.extra)['path']}}"
load_to_table = spark_job('load_to_table', cfg, 'etljobs.emr.HudiIngestor', dag, None, True, JAR_PATH)

load_to_table

if __name__ == "__main__":
    dag.cli()

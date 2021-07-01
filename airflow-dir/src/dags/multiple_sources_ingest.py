from __future__ import print_function
import airflow
from airflow.utils.dates import days_ago
from datetime import timedelta
from operators.file_operators import FileToPredictableLocationOperator
from operators.file_operators import PredictableLocationToFinalLocationOperator
from operators.file_operators import CheckReceivedFileOperator

args = {
    'owner': 'alexey',
    'start_date': days_ago(5),
    'provide_context': True
}

dag = airflow.DAG(
    'multiple_sources_ingest',
    schedule_interval='0/10 * * * * *',
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=1)

ingest_items = FileToPredictableLocationOperator(
    task_id='ingest_items',
    src_conn_id='fs_local_input',
    dst_conn_id='fs_local_raw_data',
    file_mask="items_{{ ds }}.csv",
    dag=dag)

ingest_orders = FileToPredictableLocationOperator(
    task_id='ingest_orders',
    src_conn_id='fs_local_input',
    dst_conn_id='fs_local_raw_data',
    file_mask="orders_{{ ds }}.csv",
    dag=dag)

ingest_customers = FileToPredictableLocationOperator(
    task_id='ingest_customers',
    src_conn_id='fs_local_input',
    dst_conn_id='fs_local_raw_data',
    file_mask="customers_{{ ds }}.csv",
    dag=dag)

check_file = CheckReceivedFileOperator(
    task_id='check_file',
    file_mask="*_{{ ds }}.csv",
    file_prefixes={"items", "orders", "customers"},
    dst_conn_id='fs_local_raw_data',
    dag=dag)

[ingest_items, ingest_orders, ingest_customers] >> check_file  # type: ignore

if __name__ == "__main__":
    dag.cli()

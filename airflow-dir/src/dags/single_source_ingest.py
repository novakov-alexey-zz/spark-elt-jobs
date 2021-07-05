from __future__ import print_function

from datetime import timedelta

import airflow
from airflow.utils.dates import days_ago
from operators.file_operators import CheckReceivedFileOperator
from operators.file_operators import FileToPredictableLocationOperator

args = {
    'owner': 'alexey',
    'start_date': days_ago(1),
    'provide_context': True
}

dag = airflow.DAG(
    'single_source_ingest',
    schedule_interval='0/10 * * * * *',
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=1)

file_mask = "*_{{ ds }}.csv"

ingest_file = FileToPredictableLocationOperator(
    task_id='ingest_file',
    src_conn_id='fs_local_input',
    dst_conn_id='fs_local_raw_data',
    file_mask=file_mask,
    dag=dag)

check_file = CheckReceivedFileOperator(
    task_id='check_file',
    file_mask=file_mask,
    file_prefixes={"items", "orders", "customers"},
    dst_conn_id='fs_local_raw_data',
    dag=dag)

ingest_file >> check_file

if __name__ == "__main__":
    dag.cli()

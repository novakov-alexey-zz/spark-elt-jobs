from __future__ import print_function
import airflow
from datetime import datetime, timedelta
from operators.file_operators import FileToPredictableLocationOperator
from operators.file_operators import PredictableLocationToFinalLocationOperator
from operators.file_operators import CheckReceivedFileOperator


past_date = datetime.combine(
    datetime.today() - timedelta(1),
    datetime.min.time())

args = {
    'owner': 'alexey',
    'start_date': past_date,
    'provide_context': True
}

dag = airflow.DAG(
    'file_ingest',
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
    xcom_task_id = 'ingest_file',    
    dag=dag)

ingest_file >> check_file

if __name__ == "__main__":
    dag.cli()

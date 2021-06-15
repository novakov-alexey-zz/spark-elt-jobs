import airflow
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

args = {
    'owner': 'alexey',
    'start_date': days_ago(1),
    'provide_context': True
}

dag = airflow.DAG(
    'spark_example',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=1)


operator = SparkSubmitOperator(
    task_id='file2file',
    conn_id='spark_default',
    java_class='com.ibm.cdopoc.DataLoaderDB2COS',
    application='local:///opt/spark/examples/jars/cppmpoc-dl-0.1.jar',
    total_executor_cores='1',
    executor_cores='1',
    executor_memory='2g',
    num_executors='1',
    name='airflow-file-2-file',
    verbose=False,
    driver_memory='1g',    
    dag=dag,
)

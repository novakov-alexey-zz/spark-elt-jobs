import itertools
import time
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from dags.spark_common import user_defined_macros
from datetime import timedelta
from typing import List

from airflow import DAG

args = {
    "owner": "alexey",
    "start_date": "2021-06-10"
}

dag = DAG(
    "spark_emr_hudi",
    schedule_interval=None, # or for example: '0/10 * * * * *'
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    user_defined_macros=user_defined_macros,
    max_active_runs=1,
    tags=["emr", "hudi"])


def to_list(step_args: str) -> List[str]:
    lines = step_args.strip().split("\n")
    return list(itertools.chain(*map(lambda l: l.strip().split(" "), lines)))


ingest_step_args = """
    spark-submit 
    --deploy-mode client 
    --conf spark.executor.cores=4 
    --conf spark.executor.memory=2g 
    --name load_to_tables 
    --class etljobs.emr.HudiIngestor {{fromjson(connection.s3_cloud.extra)["emrJarPath"]}}
    --entity-pattern orders:orders_*.csv:orderId:last_update_time
    --entity-pattern items:items_*.csv:itemId:last_update_time
    -i {{fromjson(connection.s3_cloud.extra)["inputPath"]}} 
    -o {{fromjson(connection.s3_cloud.extra)["rawPath"]}} 
    --execution-date {{ds}}
    -j cdc-orders 
    --overwrite 
    -s {{fromjson(connection.s3_cloud.extra)["schemaPath"]}}/cdc-orders 
    --trigger-interval -1 
    --input-format csv 
    --output-format hudi 
    --reader-options header:true     
    --partition-by year 
    --partition-by month 
    --partition-by day
    --hudi-sync-to-hive
    --sync-database poc    
"""

join_step_args = to_list("""
    spark-submit 
    --deploy-mode client 
    --conf spark.executor.cores=4 
    --conf spark.executor.memory=2g 
    --name join_tables 
    --class etljobs.emr.TableJoin {{fromjson(connection.s3_cloud.extra)["emrJarPath"]}}
    --table items         
    --table orders    
    --join-table joined:orderId:last_update_time         
    -i {{fromjson(connection.s3_cloud.extra)["rawPath"]}}
    -o {{fromjson(connection.s3_cloud.extra)["rawPath"]}}
    --execution-date {{ds}}
    -j cdc-orders         
    --input-format hudi 
    --output-format hudi         
    --partition-by year 
    --partition-by month 
    --partition-by day    
    --hudi-sync-to-hive
    --sync-database poc
    --overwrite
""") + ["--sql-join",
        "\"select o.orderId, o.customerId, o.itemId, quantity, o.year, o.month, o.day, o.last_update_time, o.execution_year, o.execution_month, o.execution_day, i.name, i.price from orders o, items i where o.itemId == i.itemId\""]

ingest_data_step = [
    {
        "Name": "Data Ingestor",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": to_list(ingest_step_args),
        },
    }
]

join_data_step = [
    {
        "Name": "Data Joiner",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": join_step_args,
        },
    }
]

bid_price = "0.15"
JOB_FLOW_OVERRIDES = {
    "Name": f"spark-emr-airflow-{time.time()}",
    "Instances": {
        "Ec2SubnetId": "subnet-e09fd18a",
        "EmrManagedMasterSecurityGroup": "sg-08b2be5eb4f33a816",
        "EmrManagedSlaveSecurityGroup": "sg-029fb4fa761512de7",
        "InstanceGroups": [
            {
                "BidPrice": bid_price,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": 32,
                                                 "VolumeType": "gp2"}, "VolumesPerInstance": 2}
                    ]
                },
                "InstanceCount": 1,
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "Market": "SPOT",
                "Name": "Core - 2"
            },
            {
                "BidPrice": bid_price,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs":
                        [
                            {"VolumeSpecification": {"SizeInGB": 32,
                                                     "VolumeType": "gp2"}, "VolumesPerInstance": 2}
                        ]
                },
                "InstanceCount": 1,
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "Name": "Master - 1"
            },
            {
                "BidPrice": bid_price,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": 32,
                                                 "VolumeType": "gp2"}, "VolumesPerInstance": 2}
                    ]},
                "InstanceCount": 1,
                "Market": "SPOT",
                "InstanceRole": "TASK",
                "InstanceType": "m4.xlarge",
                "Name": "Task - 3"
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "LogUri": "{{fromjson(connection.s3_cloud.extra)['emrLogsPath']}}",
}

# cluster_creator = EmrCreateJobFlowOperator(
#     task_id="create_job_flow",
#     job_flow_overrides=JOB_FLOW_OVERRIDES,
#     dag=dag
# )

cluster_id = "{{ dag_run.conf['cluster_id'] }}"

ingest_data = EmrAddStepsOperator(
    task_id="ingest_data",
    job_flow_id=cluster_id,  # "{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=ingest_data_step,
    dag=dag
)

wait_for_ingestor = EmrStepSensor(
    task_id="watch_ingestor",
    job_flow_id=cluster_id,  # "{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='ingest_data', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)

join_data = EmrAddStepsOperator(
    task_id="join_data",
    job_flow_id=cluster_id,  # "{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=join_data_step,
    dag=dag
)

wait_for_joiner = EmrStepSensor(
    task_id="watch_joiner",
    job_flow_id=cluster_id,  # "{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='join_data', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)

# step_terminate_cluster = EmrTerminateJobFlowOperator(
#     task_id="terminate_cluster",
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
#     aws_conn_id="aws_default",
#     dag=dag
# )

ingest_data >> wait_for_ingestor >> join_data >> wait_for_joiner
# cluster_creator >> ingest_data >> wait_for_ingestor >> join_data >> wait_for_joiner >> step_terminate_cluster

if __name__ == "__main__":
    dag.cli()

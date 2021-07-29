import itertools
import time
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
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
    "spark_emr",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    user_defined_macros=user_defined_macros,
    max_active_runs=1,
    tags=["emr"])

step_args = """
    spark-submit 
    --deploy-mode client 
    --conf spark.executor.cores=4 
    --conf spark.executor.memory=2g 
    --name load_to_table 
    --class etljobs.emr.HudiIngestor {{fromjson(connection.s3_cloud.extra)["emrJarPath"]}}
    --entity-pattern orders:orders_*.csv:orderId:last_update_time
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
"""


def to_list(step_args: str) -> List[str]:
    lines = step_args.strip().split("\n")
    return list(itertools.chain(*map(lambda l: l.strip().split(" "), lines)))


SPARK_STEPS = [
    {
        "Name": "Orders Ingestor",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": to_list(step_args),
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

cluster_creator = EmrCreateJobFlowOperator(
    task_id="create_job_flow",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag
)

step_terminate_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag
)

cluster_creator >> step_adder >> step_checker >> step_terminate_cluster

if __name__ == "__main__":
    dag.cli()

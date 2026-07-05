from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from datetime import datetime

S3_BUCKET = "pulsefleet-project-bucket-uditks"

JOB_FLOW_OVERRIDES = {
    "Name": "PulseFleet-Batch",
    "ReleaseLabel": "emr-7.13.0",
    "Applications": [{"Name": "Spark"}],
    "LogUri": f"s3://{S3_BUCKET}/emr-logs/",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_STEPS = [
    {
        "Name": "PulseFleet Batch Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                f"s3://{S3_BUCKET}/emr-scripts/emr_batch_job.py",
            ],
        },
    }
]

with DAG(
    dag_id="pulsefleet_emr_batch",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * *",
    catchup=False,
    tags=["pulsefleet", "emr", "spark"],
) as dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_batch_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    wait_for_step = EmrStepSensor(
        task_id="wait_for_batch_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_batch_step')[0] }}",
        aws_conn_id="aws_default",
    )

    wait_for_cluster_termination = EmrJobFlowSensor(
        task_id="wait_for_cluster_termination",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id="aws_default",
    )

    create_cluster >> add_steps >> wait_for_step >> wait_for_cluster_termination
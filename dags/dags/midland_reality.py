from contextvars import Context
from typing import Final

from airflow.models import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from constants.constants import GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET_NAME, GCP_CLUSTER_NAME
from utils.gcp.dataproc import get_cluster_config, get_spark_submit_job_driver

from constants.constants import S3_BUCKET_NAME
from constants.data_category import DataCategory
from constants.providers import Provider
from constants.webhook import SLACK_CONNECTION_ID, SLACK_WEBHOOK_DAILY_BATCH_BOT
from constants.dag_id import MIDLAND_PROPERTY as DAG_ID
from operators.midland_reality_sourcing import MidLandRealitySourcingOperator

from utils.date import udm_utc_to_hkt

TMP_TO_SRC_PYSPARK_URI = "gs://property-dashboard/spark-job/property_spark/app/hk_property_tmp_to_src.py"
SRC_TO_LOG0_PYSPARK_URI = "gs://property-dashboard/spark-job/property_spark/app/hk_property_src_to_log0.py"

NOTI_ON_EXECUTE_TASK_ID: Final[str] = "noti_on_execute_task"

SLACK_SUCCESS_NOTIFICATION_TASK_ID = "slack_success_notification_task_id"


def notify_success(context: Context):
    message = f""":large_green_circle: dag <{DAG_ID}> ran successfully!"""

    slack_success_notification_task = SlackWebhookOperator(
        task_id=SLACK_SUCCESS_NOTIFICATION_TASK_ID,
        http_conn_id=SLACK_CONNECTION_ID,
        webhook_token=SLACK_WEBHOOK_DAILY_BATCH_BOT,
        message=message,
    )
    return slack_success_notification_task.execute(context)

def notify_failure(context: Context):
    message = f":exclamation: dag <{DAG_ID}> failed"

    slack_failure_notification_task = SlackWebhookOperator(
        task_id=SLACK_SUCCESS_NOTIFICATION_TASK_ID,
        http_conn_id=SLACK_CONNECTION_ID,
        webhook_token=SLACK_WEBHOOK_DAILY_BATCH_BOT,
        message=message,
    )
    return slack_failure_notification_task.execute(context)

default_args = {
    "owner": "yewon",
    "start_date": "2002-08-17T14:15:23Z",
    "on_failure_callback": notify_failure,
    "retries": 3,
}

execution_date_1 = "{{ ds }}"

with DAG(
        dag_id=DAG_ID,
        catchup=False,
        schedule_interval="@daily",
        render_template_as_native_obj=True,
        tags=["main"],
        default_args=default_args,
        user_defined_macros={
            "utc_to_hkt": udm_utc_to_hkt,
        },
        on_success_callback=notify_success
) as dag:
    noti_on_execute = SlackWebhookOperator(
        task_id=NOTI_ON_EXECUTE_TASK_ID,
        http_conn_id=SLACK_CONNECTION_ID,
        webhook_token=SLACK_WEBHOOK_DAILY_BATCH_BOT,
        message=(
            "midland_property dag started"
        )
    )

    sourcing_task = MidLandRealitySourcingOperator(
        task_id="midland_reality_sourcing_task",
        provider=Provider.MIDLAND_REALITY.value,
        data_category=DataCategory.ROOM.value,
        execution_date="{{ utc_to_hkt(ts) }}",
        base_url="https://www.midland.com.hk/en/list/rent"
    )

    create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=GCP_PROJECT_ID,
            cluster_config=get_cluster_config(),
            region=GCP_REGION,
            cluster_name=GCP_CLUSTER_NAME,
            use_if_exists=True
        )
    
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=get_spark_submit_job_driver(
            main_file=TMP_TO_SRC_PYSPARK_URI,
            entry_point_arguments=["--provider-str",
                                   Provider.MIDLAND_REALITY.value,
                                    "--operation-date-str",
                                    "{{ utc_to_hkt(ts) }}",
                                    "--data-category-str",
                                    DataCategory.ROOM.value]

        ), 
        region=GCP_REGION, 
        project_id=GCP_PROJECT_ID,
    )

    pyspark_src_to_log0_task = DataprocSubmitJobOperator(
        task_id="pyspark_src_to_log0_task", 
        job=get_spark_submit_job_driver(
            main_file=SRC_TO_LOG0_PYSPARK_URI,
            entry_point_arguments=["--provider-str",
                                   Provider.HK_PROPERTY.value,
                                    "--data-category-str",
                                    DataCategory.ROOM.value]

        ), 
        region=GCP_REGION, 
        project_id=GCP_PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=GCP_PROJECT_ID, 
        cluster_name=GCP_CLUSTER_NAME, 
        region=GCP_REGION,
    )

    noti_on_execute >> sourcing_task >> create_cluster >> pyspark_task >> delete_cluster

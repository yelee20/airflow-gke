from contextvars import Context
from typing import Final

from airflow.models import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from constants.constants import S3_BUCKET_NAME
from constants.data_category import DataCategory
from constants.providers import Provider
from constants.webhook import SLACK_CONNECTION_ID, SLACK_WEBHOOK_DAILY_BATCH_BOT
from constants.dag_id import HK_PROPERTY as DAG_ID
from operators.hk_property_sourcing import HKPropertySourcingOperator

from utils.date import udm_utc_to_hkt

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
            "hk_property dag started"
        )
    )

    sourcing_task = HKPropertySourcingOperator(
        task_id="hk_property_sourcing_task",
        bucket_name=S3_BUCKET_NAME,
        provider=Provider.HK_PROPERTY.value,
        data_category=DataCategory.ROOM.value,
        execution_date="{{ utc_to_hkt(ts) }}",
    )

    noti_on_execute >> sourcing_task


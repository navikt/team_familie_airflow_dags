import os
from typing import Optional
from airflow.models import Variable

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.python import get_current_context

def slack_info(
  message: str = None,
  context = None,
  channel: str = None,
  emoji=":information_source:"
):
  if channel is None: channel = Variable.get("slack_info_channel")
  if message is None: message = f"Airflow DAG: {context['dag'].dag_id} har kjørt ferdig."
  __slack_message(context, message, channel, emoji)


def slack_error(
  context = None,
  channel: str = None,
  emoji=":red_circle:"
):
  if channel is None: channel = Variable.get("slack_error_channel")
  message = f"En Airflow DAG feilet!\n\n- DAG: {context['dag'].dag_id}\n- Task: {context['task_instance'].task_id}\n- Feilmelding: {context['exception']}"
  __slack_message(context, message, channel, emoji)


def __slack_message(
  context: str,
  message: str,
  channel: str,
  emoji: str,
  attachments: Optional[list] = None
):
  if context is None: context = get_current_context()
  SlackWebhookOperator(
    http_conn_id=None,
    task_id="slack-message",
    webhook_token=os.environ["SLACK_WEBHOOK_TOKEN"],
    message=message,
    channel=channel,
    link_names=True,
    icon_emoji=emoji,
    proxy=os.environ["HTTPS_PROXY"],
    attachments=attachments
  ).execute(context)
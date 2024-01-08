import os
from typing import Optional
from airflow.models import Variable
from kubernetes import client as k8s
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.python import get_current_context

def slack_info(message: str = None, context = None, channel: str = None, emoji=":information_source:"):
  if channel is None: 
    channel = Variable.get("slack_info_channel")
  if message is None: 
    message = f"Airflow DAG: {context['dag'].dag_id} har kjørt ferdig."
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
  SlackAPIPostOperator(
    task_id="slack-message",
    executor_config={
      "pod_override": k8s.V1Pod(
          metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
      )
    },
    slack_conn_id="slack_connection",
    text=message,
    channel=channel,
    attachments=[
      {
          "fallback": "min attachment",
          "color": "#2eb886",
          "pretext": "test",
          "author_name": "Nada",
          "title": "Nada",
          "text": "test",
          "fields": [
              {
                  "title": "Priority",
                  "value": "High",
                  "short": False
              }
          ],
          "footer": "Nada"
      }
    ]
  )

from datetime import datetime

from airflow.models import DAG
from airflow.models import Variable

from kosument_config import ks
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator

import pendulum

with DAG(
  dag_id="syfo_isdialogmote_consumer",
  start_date=datetime(2022, 8, 9),
  schedule_interval="@hourly",
  max_active_runs=1,
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id="isdialogmote-kafka-consumer",
    config=ks.config,
    slack_channel=Variable.get("slack_error_channel"),
  )

consumer
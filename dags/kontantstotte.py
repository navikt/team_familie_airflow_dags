from datetime import datetime

from airflow.models import DAG
from airflow.models import Variable

from kosument_config import ks
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator

import pendulum

with DAG(
  dag_id="fam_ef_consumer_test",
  start_date=datetime(2022, 11, 15),
  schedule_interval="@hourly",
  max_active_runs=1
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "ef-kafka-consumer_test",
    config = ks.config,
    #data_interval_start_timestamp_milli="1634688000000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    #data_interval_end_timestamp_milli="1668470400000",   # from first day we got data until 15.11.2022 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )

consumer
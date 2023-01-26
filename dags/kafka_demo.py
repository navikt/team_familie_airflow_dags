from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import config_demo
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator

with DAG(
  dag_id="kafka_demo",
  start_date=datetime(2023, 1, 25),
  schedule_interval= None,
  max_active_runs=1
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "hent_data_fra_topic",
    config = config_demo.config,
    data_interval_start_timestamp_milli="1672586518000", # 01.01.2023
    data_interval_end_timestamp_milli="1674228118000",   # 20.01.2023
    slack_channel = Variable.get("slack_error_channel")
  )

consumer
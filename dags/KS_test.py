from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import ks_test
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator

with DAG(
  dag_id = "KS_les_data",
  start_date = datetime(2023, 1, 23),
  schedule_interval = None,#"@hourly",
  max_active_runs = 1
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "KS_last_data_test",
    config = ks_test.config,
    data_interval_start_timestamp_milli="1667260800000", # 01.11.2022
    data_interval_end_timestamp_milli="1674172800000",   # 20.01.2023
    slack_channel = Variable.get("slack_error_channel")
  )

consumer
from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import test
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator

with DAG(
  dag_id="kafka_test",
  start_date=datetime(2023, 1, 25),
  schedule_interval= None,
  max_active_runs=1
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "hent_data_fra_topic",
    config = test.config,
    data_interval_start_timestamp_milli="1673308800000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    data_interval_end_timestamp_milli="1674604800000",   # from first day we got data until 15.11.2022 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )

consumer
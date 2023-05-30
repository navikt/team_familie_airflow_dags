from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import barnetrygd
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator

with DAG(
  dag_id="barnetrygd_read_kafka_topic",
  start_date=datetime(2023, 5, 23),
  schedule_interval= None, #"@hourly",
  max_active_runs=1
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "barnetrygd_hent_kafka_data",
    config = barnetrygd.config,
    data_interval_start_timestamp_milli="1684022400000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    data_interval_end_timestamp_milli="1685318400000",   # from first day we got data until 29.05.2023 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )

consumer


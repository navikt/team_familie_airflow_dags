from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import fp
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator
from operators.slack_operator import slack_error
from allowlists.allowlist import prod_oracle_conn_id, dev_oracle_conn_id

miljo = Variable.get('miljo')

allowlist = []
if miljo == 'Prod':
  allowlist.extend(prod_oracle_conn_id)
else:
  allowlist.extend(dev_oracle_conn_id)

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_fp_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

topic = Variable.get("FP_topic") # topic navn hentes foreløpig fra airflow variabler "teamforeldrepenger.fpsak-dvh-stonadsstatistikk-v1" 

with DAG(
  dag_id="FP_konsument_test",
  start_date=datetime(2024, 4, 11),
  default_args = default_args,
  schedule_interval= "@hourly",
  max_active_runs=1,
  catchup = True
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "foreldrepenger_hent_kafka_data_test",
    config = fp.config.format(topic),
    #data_interval_start_timestamp_milli="1708955634000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    #data_interval_end_timestamp_milli="1709301234000",   # from first day we got data until 29.05.2023 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )


consumer
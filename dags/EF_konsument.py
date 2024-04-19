from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import ef
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, dev_oracle_conn_id,r_oracle_conn_id

miljo = Variable.get('miljo')
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

topic = Variable.get("EF_topic") # topic navn hentes foreløpig fra airflow variabler "teamfamilie.aapen-ensligforsorger-vedtak-test" 

with DAG(
  dag_id="EF_konsument",
  start_date=datetime(2024, 4, 18, 12),
  default_args = default_args,
  schedule_interval= "@hourly",
  max_active_runs=1,
  catchup = False
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "ensligforsorger_hent_kafka_data",
    config = ef.config.format(topic),
    #data_interval_start_timestamp_milli="1712916000000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    #data_interval_end_timestamp_milli="1713448800000",   # from first day we got data until 29.05.2023 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )

  ef_utpakking_dbt = create_dbt_operator(
     dag=dag,
     name="utpakking_ef",
     script_path = 'airflow/dbt_run.py',
     branch=v_branch,
     dbt_command= """run --select EF_utpakking.*""",
     db_schema=v_schema,
     allowlist=allowlist

 )

consumer >> ef_utpakking_dbt
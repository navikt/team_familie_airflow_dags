from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import up
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, dev_oracle_conn_id,r_oracle_conn_id
from felles_metoder.felles_metoder import parse_task_image

miljo = Variable.get('miljo')
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id) #dev

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_up_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

topic = Variable.get("UP_topic") 

with DAG(
  dag_id="UP_konsument",
  start_date=datetime(2026, 1, 6),
  default_args = default_args,
  schedule_interval= "@hourly",
  max_active_runs=1,
  catchup = True
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "ungdomsprogram_hent_kafka_data",
    config = up.config.format(topic),
    kafka_consumer_image=parse_task_image("dvh-airflow-kafka"),
    #data_interval_start_timestamp_milli="1712916000000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    #data_interval_end_timestamp_milli="1713448800000",   # from first day we got data until 29.05.2023 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )

  up_utpakking_dbt = create_dbt_operator(
     dag=dag,
     name="utpakking_up",
     repo='navikt/dvh_fam_ungdom',
     script_path = 'airflow/dbt_run.py',
     branch=v_branch,
     dbt_command= """run --select UP_utpakking.*""",
     db_schema=v_schema,
     allowlist=allowlist
 )

consumer >> up_utpakking_dbt
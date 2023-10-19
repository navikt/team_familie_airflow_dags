from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import pp
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_pp_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

with DAG(
  dag_id="PP_konsument",
  start_date=datetime(2023, 10, 19),
  schedule_interval= "@hourly",
  max_active_runs=1,
  catchup = False
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "pleiepenger_hent_kafka_data",
    config = pp.config,
    #data_interval_start_timestamp_milli="1684022400000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    #data_interval_end_timestamp_milli="1685318400000",   # from first day we got data until 29.05.2023 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )

  pp_utpakking_dbt = create_dbt_operator(
      dag=dag,
      name="utpakking_pp",
      script_path = 'airflow/dbt_run.py',
      branch=v_branch,
      dbt_command= "run --select PP_utpakking.*",
      db_schema=v_schema
  )

consumer >> pp_utpakking_dbt


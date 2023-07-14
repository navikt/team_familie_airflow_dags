from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import barnetrygd
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_bt_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

with DAG(
  dag_id="barnetrygd_read_kafka_topic",
  start_date=datetime(2023, 5, 28),
  schedule_interval= "@hourly",
  max_active_runs=1
) as dag:

  consumer = kafka_consumer_kubernetes_pod_operator(
    task_id = "barnetrygd_hent_kafka_data",
    config = barnetrygd.config,
    #data_interval_start_timestamp_milli="1684022400000", # gir oss alle data som ligger på topicen fra og til (intial last alt på en gang)
    #data_interval_end_timestamp_milli="1685318400000",   # from first day we got data until 29.05.2023 (todays before todays date)
    slack_channel = Variable.get("slack_error_channel")
  )

  bt_utpakking_dbt = create_dbt_operator(
      dag=dag,
      name="utpakking_bt",
      script_path = 'airflow/dbt_run.py',
      branch=v_branch,
      dbt_command= """run --select BT_utpakking.* --vars "{{dag_interval_start: '{0}', dag_interval_end: '{1}'}}" """.format('{{ execution_date.in_timezone("Europe/Amsterdam").strftime("%Y-%m-%d %H:%M:%S")}}','{{ (execution_date + macros.timedelta(hours=1)).in_timezone("Europe/Amsterdam").strftime("%Y-%m-%d %H:%M:%S")}}'),
      db_schema=v_schema
  )

consumer >> bt_utpakking_dbt


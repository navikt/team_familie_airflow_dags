from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from kosument_config import ef
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error
from allowlists.allowlist import prod_oracle_conn_id, dev_oracle_conn_id
from siste_image_versjon import get_latest_ghcr_tag


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
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

topic = Variable.get("EF_topic") # topic navn hentes foreløpig fra airflow variabler "teamfamilie.aapen-ensligforsorger-vedtak-test" 
slack_channel = Variable.get("slack_error_channel")


def siste_image_versjon(ti):
  repo = "dvh-airflow-kafka" #"dvh-images/airflow-dbt"
  siste_image_versjon = get_latest_ghcr_tag(repo)
  ti.xcom_push(key='siste_versjon', value = siste_image_versjon)


def fetch_latest_image_version(ti):
    #Fetches the latest GHCR tag and pushes to XCom.
    repo = "dvh-airflow-kafka"
    siste_image_versjon = get_latest_ghcr_tag(repo)
    ti.xcom_push(key='siste_versjon', value=siste_image_versjon)

def build_kafka_task(ti):
    #Builds the Kafka consumer task using the latest image version.
    latest_version = ti.xcom_pull(task_ids='fetch_image_version', key='siste_versjon')
    return kafka_consumer_kubernetes_pod_operator(
        task_id="consume_kafka_data",
        config=ef.config.format(topic),
        image=f"ghcr.io/navikt/dvh-airflow-kafka:{latest_version}",
        slack_channel=slack_channel
    )


with DAG(
  dag_id="EF_konsument",
  start_date=datetime(2023, 8, 7),
  default_args = default_args,
  schedule_interval= "@hourly",
  max_active_runs=1,
  catchup = True
) as dag:
  
  fetch_image_version = PythonOperator(
      task_id="fetch_image_version",
      python_callable=fetch_latest_image_version,
  )

  kafka_consumer = PythonOperator(
      task_id="build_kafka_consumer",
      python_callable=build_kafka_task,
  )

  run_dbt = create_dbt_operator(
      dag=dag,
      name="run_ef_utpakking",
      script_path='airflow/dbt_run.py',
      branch=v_branch,
      dbt_command="run --select EF_utpakking.*",
      db_schema=v_schema,
      allowlist=allowlist
  )

  fetch_image_version >> kafka_consumer >> run_dbt
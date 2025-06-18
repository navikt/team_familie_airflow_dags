from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kubernetes import client
from operators.slack_operator import slack_error
from allowlists.allowlist import prod_oracle_conn_id, dev_oracle_conn_id,r_oracle_conn_id

from dataverk_airflow import python_operator
from confluent_kafka import Consumer

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

#Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_bb_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

topic = Variable.get("BB_topic_ord") # topic navn hentes foreløpig fra airflow variabler "teamfamilie.aapen-ensligforsorger-vedtak-test" 

with DAG(
  dag_id="BB_ord_headers",
  start_date=datetime(2025, 6, 16, 6),
  default_args = default_args,
  schedule_interval= None,
  max_active_runs=1,
  catchup = False
) as dag:

    python_requirement = python_operator(
        dag=dag,
        name="installasjon_requirement",
        repo="navikt/team_familie_airflow_dags",
        script_path="Oracle_python/test_hello.py",
        branch=v_branch,
        resources=client.V1ResourceRequirements(
            requests={"memory": "4G"},
            limits={"memory": "4G"}),
        slack_channel=Variable.get("slack_error_channel"),
        requirements_path="Oracle_python/requirements.txt"
    )

    #consumer = KafkaConsumer(
    #    topic,
    #    bootstrap_servers=['nav-dev-kafka-139'],
    #    group_id='dvh_familie_konsument',
    #    # Add other necessary configurations (e.g., security, deserializers)
    #)

    #for message in consumer:
    #    headers = message.bb
    #    for header in headers:
    #        print(f"Header Key: {header.key}, Header Value: {header.value}")

python_requirement #>> consumer
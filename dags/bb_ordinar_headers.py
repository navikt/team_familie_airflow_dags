from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import bb
from operators.kafka_operators import kafka_consumer_kubernetes_pod_operator
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error
from allowlists.allowlist import prod_oracle_conn_id, dev_oracle_conn_id,r_oracle_conn_id
from felles_metoder.felles_metoder import parse_task_image

from kafka import KafkaConsumer

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

topic = Variable.get("BB_topic_ord") # topic navn hentes foreløpig fra airflow variabler "teamfamilie.aapen-ensligforsorger-vedtak-test" 

with DAG(
  dag_id="BB_ord_headers",
  start_date=datetime(2025, 6, 16, 6),
  default_args = default_args,
  schedule_interval= None,
  max_active_runs=1,
  catchup = False
) as dag:


    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['nav-dev-kafka-139'],
        group_id='dvh_familie_konsument',
        # Add other necessary configurations (e.g., security, deserializers)
    )

    for message in consumer:
        headers = message.headers
        for header in headers:
            print(f"Header Key: {header.key}, Header Value: {header.value}")

consumer
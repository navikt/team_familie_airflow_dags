from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from operators.slack_operator import slack_info, slack_error

from operators.dbt_operator import create_dbt_operator

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]


with DAG('hans-datalast', 
        default_args=default_args,
        schedule_interval = None,  
        start_date = datetime(2022, 11, 21),
        catchup = False
        ) as dag:


    hans_run_dbt = create_dbt_operator(
        dag=dag,
        name="unpack_all_new_kafka_ko",
        branch=v_branch,
        dbt_command="run -m tag:ef_kafka_test",
        db_schema=v_schema
    )
hans_run_dbt
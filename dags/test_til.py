from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
import os
from operators.slack_operator import slack_error
from airflow.decorators import task
from dataverk_airflow.knada_operators import create_knada_python_pod_operator


#------- Steg 1 -------#
default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
}


#------- Steg 2 -------#
with DAG(
    dag_id = 'en_test_til', 
    description = 'skriv en beskrivelse av hva dagen gjør',
    default_args = default_args,
    start_date = datetime(2023, 4, 1), # start date for the dag
    schedule_interval = None,
    catchup = False # slipper å ha alle kjøringer mellom start_date og dagens dato (default er true)
) as dag:
    
    insert_data = create_knada_python_pod_operator(
    dag=dag,
    name="insert_data_til_tabell",
    repo="navikt/team_familie_airflow_dags",
    script_path="Oracle_python/insert_test_data.py",
    branch="dev",
    delete_on_finish= False,
    resources=client.V1ResourceRequirements(
        requests={"memory": "4G"},
        limits={"memory": "4G"}),
    slack_channel=Variable.get("slack_error_channel")
    )

    insert_data

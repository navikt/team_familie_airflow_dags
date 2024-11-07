from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import requests
from datetime import datetime, timedelta

# api_conn variabler
api_conn = Variable.get("api_conn", deserialize_json=True)
user = api_conn["user"]
passord = api_conn["passord"]

def check_and_rerun_failed_dags(dag_id_list):
    # Airflow API endpoint 
    airflow_api_url = 'http://airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns'

    # hent dato for siste 7 dager
    one_week_ago = datetime.now() - timedelta(weeks=1)

    # Get the Airflow connection for authentication if needed
    #airflow_conn = BaseHook.get_connection('your_airflow_connection')
    #auth = (airflow_conn.login, airflow_conn.password)

    auth = (user,passord)
    
    for dag_id in dag_id_list:
        # Request for å hente DAG runs for en DAG
        response = requests.get(airflow_api_url.format(dag_id=dag_id), auth=auth)

        if response.status_code == 200:
            dag_runs = response.json().get('dagRuns', [])
            print(dag_runs)
            for run in dag_runs:
                # hent execution_date og så konverter den til datetime
                execution_date = datetime.fromisoformat(run['execution_date'].replace('Z', '+00:00'))

                print("execution_date: .:{}".format(execution_date))
                
                # Sjekk om denne kjøringen er feilet og at den har skjedd i den siste uka
                if execution_date >= one_week_ago and run['state'] == 'failed':
                    # Rekjører den DAG run som feilet
                    rerun_url = f'{airflow_api_url.format(dag_id=dag_id)}/run'
                    payload = {"execution_date": run['execution_date']}
                    rerun_response = requests.post(rerun_url, json=payload, auth=auth)

                    if rerun_response.status_code == 200:
                        print(f"DAG-en {dag_id} som feilet er blitt rekjørt for execution_date {run['execution_date']}")
                    else:
                        print(f"Klarte ikke å rekjøre DAG-en {dag_id} for execution_date {run['execution_date']}: {rerun_response.status_code}")
        else:
            print(f"Failet å hente dag-runs for DAG-en {dag_id}: {response.status_code}")

dag_id_list = ['BT_konsument', 'KS_konsument', 'EF_konsument']  # Replace with your actual DAG IDs

default_args = {
    'owner': 'Team-Familie',
    'start_date': datetime(2024, 11, 6),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'rekjor_feilet_dags',
    default_args=default_args,
    schedule_interval = None,  
)

check_failed_dags_task = PythonOperator(
    task_id='rekjor_failet_dager',
    python_callable=check_and_rerun_failed_dags,
    op_kwargs={'dag_id_list': dag_id_list},
    dag=dag,
)

check_failed_dags_task

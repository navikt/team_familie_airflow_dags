from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from datetime import datetime, timedelta

def check_and_rerun_failed_dags(dag_id_list):
    # Airflow API endpoint (adjust the URL to match your Airflow web server)
    airflow_api_url = 'http://airflow-webserver.team-familie-test-r-avzk.svc.cluster.local:8080/api/v1/dags/{dag_id}/dagRuns'

    # Get the date range for the last week
    one_week_ago = datetime.now() - timedelta(weeks=1)
    
    for dag_id in dag_id_list:
        # Request to get the DAG runs for the given DAG
        response = requests.get(airflow_api_url.format(dag_id=dag_id))

        if response.status_code == 200:
            dag_runs = response.json().get('dagRuns', [])
            for run in dag_runs:
                # Get the execution_date and convert it from string to datetime
                execution_date = datetime.fromisoformat(run['execution_date'].replace('Z', '+00:00'))
                
                # Check if the run is in the last week and has failed
                if execution_date >= one_week_ago and run['state'] == 'failed':
                    # Rerun the failed DAG run
                    rerun_url = f'{airflow_api_url.format(dag_id=dag_id)}/run'
                    payload = {"execution_date": run['execution_date']}
                    rerun_response = requests.post(rerun_url, json=payload)

                    if rerun_response.status_code == 200:
                        print(f"Successfully reran failed DAG {dag_id} for execution_date {run['execution_date']}")
                    else:
                        print(f"Failed to rerun DAG {dag_id} for execution_date {run['execution_date']}: {rerun_response.status_code}")
        else:
            print(f"Failed to fetch runs for DAG {dag_id}: {response.status_code}")

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
    schedule_interval = None,  # Adjust as needed
)

check_failed_dags_task = PythonOperator(
    task_id='rekjor_failet_dager',
    python_callable=check_and_rerun_failed_dags,
    op_kwargs={'dag_id_list': dag_id_list},
    dag=dag,
)

check_failed_dags_task

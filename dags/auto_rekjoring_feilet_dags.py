from airflow import DAG
from dataverk_airflow import python_operator
from airflow.hooks.base_hook import BaseHook
import requests
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from kubernetes import client
from operators.slack_operator import slack_info
from airflow.decorators import task
from dataverk_airflow import python_operator
from allowlists.allowlist import slack_allowlist, dev_oracle_conn_id, prod_oracle_conn_id,r_oracle_conn_id

miljo = Variable.get('miljo')   
allowlist = []
if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'R':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)

def check_and_rerun_failed_dags(dag_id_list):
    # Airflow API endpoint (adjust the URL to match your Airflow web server)
    airflow_api_url = 'http://your-airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns'

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


dag_id_list = ['BT_konsument', 'KS_konsument', 'EF_konsument', ]  # Replace with your actual DAG IDs

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'rekjør_feilet_dags',
    default_args=default_args,
    schedule_interval=None,  
)

@task(
    executor_config={
        "pod_override": client.V1Pod(
            metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(slack_allowlist)})
        )
    }
)
def notification_start():
    slack_info(
        message = "kjøring av feilet dag runs starter nå! :rocket:"
    )

start_alert = notification_start()

check_failed_dags_task = python_operator(
    dag=dag,
    name="rekjør_failet_dager",
    python_callable=check_and_rerun_failed_dags,
    op_kwargs={'dag_id_list': dag_id_list},
    allowlist=allowlist,
    resources=client.V1ResourceRequirements(
        requests={"memory": "4G"},
        limits={"memory": "4G"}),
    slack_channel=Variable.get("slack_error_channel")
)

@task(
    executor_config={
        "pod_override": client.V1Pod(
            metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(slack_allowlist)})
        )
    }
)
def notification_end():
    slack_info(
        message = "kjøring av feilet dag runs er ferdig! :tada: :tada:"
    )
slutt_alert = notification_end()

start_alert >> check_failed_dags_task >> slutt_alert



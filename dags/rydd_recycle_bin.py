from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
from operators.slack_operator import slack_error
from airflow.decorators import task
from dataverk_airflow import python_operator
from allowlists.allowlist import prod_oracle_conn_id, dev_oracle_conn_id


branch = Variable.get("branch")

miljo = Variable.get('miljo')

allowlist = []
if miljo == 'Prod':
  allowlist.extend(prod_oracle_conn_id)
else:
  allowlist.extend(dev_oracle_conn_id)

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

with DAG(
    dag_id = 'rydd_recycle_bin', 
    description = 'An Airflow DAG that deletes dbt_temp tables i recycle bin',
    default_args = default_args,
    start_date = datetime(2024, 6, 3), 
    schedule_interval = '@daily' , 
    catchup = False 
) as dag:
    
    slett_tabeller_recycle_bin = python_operator(
    dag=dag,
    name="slett_dbt_tabeller_recycle_bin",
    repo="navikt/team_familie_airflow_dags",
    script_path="Oracle_python/slett_fra_recycle_bin.py",
    branch=branch,
    resources=client.V1ResourceRequirements(
        requests={"memory": "4G"},
        limits={"memory": "4G"}),
    slack_channel=Variable.get("slack_error_channel"),
    requirements_path="Oracle_python/requirements.txt",
    allowlist = allowlist
)
    
slett_tabeller_recycle_bin
    

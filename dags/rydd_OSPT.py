from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
from operators.slack_operator import slack_error
from airflow.decorators import task
from dataverk_airflow import python_operator
from allowlists.allowlist import prod_oracle_conn_id, dev_oracle_conn_id

# This DAG is usually an indication the DBT job crashed, therefore not deleting the temp table. 
# This is a reactive solution, but we should also spend time looking for a proactive solution to these crashes. 

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
    dag_id = 'rydd_OSPT', # The key 'rydd_O$PT' has to be made of alphanumeric characters, dashes, dots and underscores exclusively
    description = 'An Airflow DAG that deletes OSPT-tables',
    default_args = default_args,
    start_date = datetime(2024, 7, 10),
    schedule_interval = "0 10 * * *", # kl 12 CEST hver dag, ønsker at denne skal kjøre etter alle daglige konsumenter
    catchup = False 
) as dag:
    
    slett_tabeller_recycle_bin = python_operator(
    dag=dag,
    name="slett_OSPT",
    repo="navikt/team_familie_airflow_dags",
    script_path="Oracle_python/slett_OSPT.py",
    branch=branch,
    resources=client.V1ResourceRequirements(
        requests={"memory": "4G"},
        limits={"memory": "4G"}),
    slack_channel=Variable.get("slack_error_channel"),
    requirements_path="Oracle_python/requirements.txt",
    allowlist = allowlist
)
    
slett_tabeller_recycle_bin
    

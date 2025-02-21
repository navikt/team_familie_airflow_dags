from airflow.models import DAG, Variable
from allowlists.allowlist import slack_allowlist, dev_oracle_conn_id, prod_oracle_conn_id
from airflow.utils.dates import datetime, timedelta
from operators.slack_operator import slack_info, slack_error
from dataverk_airflow import python_operator
from airflow.decorators import task
from kubernetes import client
from Oracle_python import ssb_fp
from utils.db.oracle_conn import oracle_conn

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
    dag_id = 'SSB_FP', 
    description = 'SSB dag for foreldrepenger',
    default_args = default_args,
    start_date = datetime(2025, 2, 21), # start date for the dag
    schedule_interval = None,#'0 0 5 * *' , # 5te hver måned,
    catchup = False # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running
) as dag:
    hent_data_fra_oracle_fp_task = python_operator(
            dag=dag,
            name="hent_data_fra_oracle_fp",
            repo="navikt/team_familie_airflow_dags",
            script_path="Oracle_python/ssb_fp.py",
            branch=branch,
            allowlist=allowlist,
            resources=client.V1ResourceRequirements(
                requests={"memory": "4G"},
                limits={"memory": "4G"}),
            slack_channel=Variable.get("slack_error_channel"),
            requirements_path="Oracle_python/requirements.txt"
        )

hent_data_fra_oracle_fp_task
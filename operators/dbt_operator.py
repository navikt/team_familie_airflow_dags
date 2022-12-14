import os

from airflow.models import Variable
from kubernetes import client
from airflow import DAG
from dataverk_airflow.knada_operators import create_knada_python_pod_operator

def create_dbt_operator(
  dag: DAG,
  name: str,
  branch: str,
  dbt_command: str,
  db_schema: str,
  script_path:str,
  *args,
  **kwargs):


  return create_knada_python_pod_operator(
    dag=dag,
    name=name,
    repo='navikt/dvh_familie_dbt',
    script_path=script_path,#'airflow/dbt_run_test.py',
    branch=branch,
    do_xcom_push=True,
    resources=client.V1ResourceRequirements(
        requests={"memory": "4G"},
        limits={"memory": "4G"}
        ),
    extra_envs={
      'DBT_COMMAND': dbt_command,
      'LOG_LEVEL': 'DEBUG',
      'DB_SCHEMA': db_schema,
      'KNADA_TEAM_SECRET': os.getenv('KNADA_TEAM_SECRET')
    },
    slack_channel=Variable.get("slack_error_channel")
  )


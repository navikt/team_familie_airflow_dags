import os

from airflow.models import Variable
from kubernetes import client
from airflow import DAG
from dataverk_airflow import python_operator

def create_dbt_operator(
  dag: DAG,
  name: str,
  branch: str,
  dbt_command: str,
  db_schema: str,
  script_path:str,
  allowlist: list = [],
  *args,
  **kwargs):


  return python_operator(
    dag=dag,
    name=name,
    repo='navikt/dvh_familie_dbt',
    script_path=script_path,#'airflow/dbt_run_test.py',
    branch=branch,
    do_xcom_push=True,
    resources=client.V1ResourceRequirements(
        requests={"memory": "6G"},
        limits={"memory": "6G"}
        ),
    extra_envs={
      'DBT_COMMAND': dbt_command,
      'LOG_LEVEL': 'DEBUG',
      'DB_SCHEMA': db_schema,
      'KNADA_TEAM_SECRET': os.getenv('KNADA_TEAM_SECRET'),
      "ORA_PYTHON_DRIVER_TYPE": "thin"
    },
    slack_channel=Variable.get("slack_error_channel"),
    #requirements_path="requirements.txt",
    image='ghcr.io/navikt/dvh-images/airflow-dbt:2024-10-11-c0f1e0b',
    allowlist = allowlist
  )


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
  repo:str,
  db_schema: str,
  script_path:str,
  allowlist: list | None = None,
  execution_timeout =None,
  publish_docs: bool = False,
  *args,
  **kwargs):
  
  if allowlist is None:
      allowlist = []

  if execution_timeout is not None:
      kwargs["execution_timeout"] = execution_timeout

  env_vars = {
      'DBT_COMMAND': dbt_command,
      'LOG_LEVEL': 'DEBUG',
      'DB_SCHEMA': db_schema,
      'KNADA_TEAM_SECRET': os.getenv('KNADA_TEAM_SECRET'),
      "ORA_PYTHON_DRIVER_TYPE": "thin"
    }
    
  if publish_docs:
    env_vars["DBT_DOCS_URL"] = Variable.get("DBT_DOCS_URL")  

  return python_operator(
    dag=dag,
    name=name,
    repo=repo, #'navikt/dvh_familie_dbt',
    script_path=script_path,#'airflow/dbt_run_test.py',
    branch=branch,
    do_xcom_push=True,
    resources=client.V1ResourceRequirements(
        requests={"memory": "10G", "cpu": "8"},
        limits={"memory": "10G", "cpu": "8"}
        ),
    extra_envs=env_vars,
    slack_channel=Variable.get("slack_error_channel"),
    #requirements_path="requirements.txt",
    image='ghcr.io/navikt/dvh-images/airflow-dbt:20260318-153644', 
    allowlist = allowlist,
    **kwargs
  )


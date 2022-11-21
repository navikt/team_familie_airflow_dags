import os
from airflow import DAG
from airflow.models import Variable
from dataverk_airflow.knada_operators import create_knada_python_pod_operator

def create_dbt_operator(
  dag: DAG,
  name: str,
  branch: str,
  dbt_command: str,
  db_schema: str,
  *args,
  **kwargs):

  os.environ["KNADA_PYTHON_POD_OP_IMAGE"] = "ghcr.io/navikt/knada-airflow:2022-10-28-bc29840"

  return create_knada_python_pod_operator(
    dag=dag,
    name=name,
    repo='navikt/dvh_familie_dbt',
    script_path='airflow/dbt_run_test.py',
    namespace=Variable.get("NAMESPACE"),
    branch=branch,
    do_xcom_push=True,
    extra_envs={
      'DBT_COMMAND': dbt_command,
      'LOG_LEVEL': 'DEBUG',
      'DB_SCHEMA': db_schema
    },
    slack_channel=Variable.get("slack_error_channel")
  )
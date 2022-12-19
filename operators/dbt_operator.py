import os

from datetime import timedelta
from pathlib import Path
from typing import Callable
from airflow.models import Variable
from kubernetes import client

from airflow import DAG
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from dataverk_airflow.knada_operators import create_knada_python_pod_operator

from dataverk_airflow.init_containers import create_git_clone_init_container
from dataverk_airflow.notifications import create_email_notification, create_slack_notification



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


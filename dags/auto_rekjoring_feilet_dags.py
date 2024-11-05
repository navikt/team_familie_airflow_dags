
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from kubernetes import client
from operators.slack_operator import slack_info
from airflow.decorators import task
from dataverk_airflow import python_operator
from allowlists.allowlist import slack_allowlist, dev_oracle_conn_id, prod_oracle_conn_id,r_oracle_conn_id

branch = Variable.get("branch")

miljo = Variable.get('miljo')   
allowlist = []
if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'R':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)

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
    repo="navikt/team_familie_airflow_dags",
    script_path="Oracle_python/rekjoring_dags.py",
    allowlist=allowlist,
    branch=branch,
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



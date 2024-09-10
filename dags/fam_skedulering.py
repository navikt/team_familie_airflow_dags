from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from dataverk_airflow import python_operator
from allowlists.allowlist import slack_allowlist, dev_oracle_conn_id, prod_oracle_conn_id, r_oracle_conn_id

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
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

with DAG(
    dag_id = 'Fam_skedulering', 
    description = 'Familie felles skedulering som kaller plsql',
    default_args = default_args,
    start_date = datetime(2024, 8, 30), # start date for the dag
    schedule_interval = '0 8 * * *',#'@daily', # 5te hver måned,
    catchup = False # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running
) as dag:

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(slack_allowlist)})
            )
        }
    )
    def notification_start():
        slack_info(
            message = "Familie felles skedulering! :rocket:"
        )

    start_alert = notification_start()

    fam_skedulering = python_operator(
        dag=dag,
        name="fam_skedulering",
        repo="navikt/team_familie_airflow_dags",
        script_path="Oracle_python/fam_skedulering.py",
        branch=branch,
        allowlist=allowlist,
        resources=client.V1ResourceRequirements(
            requests={"memory": "4G"},
            limits={"memory": "4G"}),
        requirements_path="Oracle_python/requirements.txt",
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
            message = "Familie felles skedulering er nå ferdig kjørt! :tada: :tada:"
        )
    slutt_alert = notification_end()

start_alert >> fam_skedulering >> slutt_alert
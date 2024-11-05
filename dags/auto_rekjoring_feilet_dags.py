
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from kubernetes import client
from operators.slack_operator import slack_error,slack_info
from airflow.decorators import task
from dataverk_airflow import python_operator
from allowlists.allowlist import slack_allowlist

branch = Variable.get("branch")

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

with DAG(
    dag_id = 'rekjør_feilet_dags', 
    description = 'An Airflow DAG that invokes "FAM_EF.fam_ef_patch_infotrygd_arena" og "FAM_EF.fam_ef_patch_migrering_vedtak" stored procedures',
    default_args = default_args,
    start_date = datetime(2024, 11, 4), # start date for the dag
    schedule_interval = None , # 5te hver måned,
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
            message = "Patching av ybarn og migrerte_vedtak starter nå ved hjelp av Airflow! :rocket:"
        )

    start_alert = notification_start()


    check_failed_dags_task = python_operator(
        dag=dag,
        name="rekjør_failet_dager",
        repo="navikt/team_familie_airflow_dags",
        script_path="Oracle_python/rekjoring_dags.py",
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

check_failed_dags_task



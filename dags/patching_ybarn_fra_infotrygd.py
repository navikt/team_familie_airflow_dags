from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
import os
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from dataverk_airflow.knada_operators import create_knada_python_pod_operator


default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

with DAG(
    dag_id = 'Fam_EF_patching_ybarn_arena', 
    description = 'An Airflow DAG that invokes "FAM_EF.fam_ef_patch_infotrygd_arena" stored procedure',
    default_args = default_args,
    start_date = datetime(2022, 10, 1), # start date for the dag
    schedule_interval = None,#'0 0 5 * *' , # 5te hver måned,
    catchup = False # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running
) as dag:


    @task
    def notification_start():
        slack_info(
            message = "Patching av ybarn til fam_ef_stonad tabellen starter nå ved hjelp av Airflow! :rocket:"
        )

    start_alert = notification_start()

    patch_ybarn_arena = create_knada_python_pod_operator(
        dag=dag,
        name="fam_ef_patch_ybarn_infotrygd_arena",
        repo="navikt/team_familie_airflow_dags",
        script_path="Oracle_python/fam_ef_patch_ybarn.py",
        branch="test_r",
        resources=client.V1ResourceRequirements(
            requests={"memory": "4G"},
            limits={"memory": "4G"}),
        slack_channel=Variable.get("slack_error_channel")
    )

    @task
    def notification_end():
        slack_info(
            message = "Fam_Ef_patch_ybarn_infotrygd_arena er nå ferdig kjørt! :tada: :tada:"
        )
    slutt_alert = notification_end()

start_alert >> patch_ybarn_arena >> slutt_alert

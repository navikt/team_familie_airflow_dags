from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from dataverk_airflow.knada_operators import create_knada_python_pod_operator

branch = Variable.get("branch")

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

with DAG(
    dag_id = 'FAM_BT_SKEDULERING',
    description = 'Barnetrygd skedulering',
    default_args = default_args,
    start_date = datetime(2023, 9, 13), # start date for the dag
    schedule_interval = None,#'0 0 5 * *' , # 5te hver måned,
    catchup = True # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running
) as dag:

    @task
    def notification_start():
        slack_info(
            message = "Patching av fk_person1 for BT starter nå ved hjelp av Airflow! :rocket:"
        )

    start_alert = notification_start()

    patch_fk_person1 = create_knada_python_pod_operator(
        dag=dag,
        name="fam_bt_patch_fk_person1",
        repo="navikt/team_familie_airflow_dags",
        script_path="Oracle_python/fam_bt_skedulering.py",
        branch=branch,
        resources=client.V1ResourceRequirements(
            requests={"memory": "4G"},
            limits={"memory": "4G"}),
        slack_channel=Variable.get("slack_error_channel")
    )

    @task
    def notification_end():
        slack_info(
            message = "Patching av fk_person1 for BT er nå ferdig kjørt! :tada: :tada:"
        )
    slutt_alert = notification_end()

start_alert >> patch_fk_person1 >> slutt_alert


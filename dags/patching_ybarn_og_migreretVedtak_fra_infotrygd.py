from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
from operators.slack_operator import slack_info, slack_error
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
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

with DAG(
    dag_id = 'Fam_EF_patching_ybarn_og_migrerte_vedtak', 
    description = 'An Airflow DAG that invokes "FAM_EF.fam_ef_patch_infotrygd_arena" og "FAM_EF.fam_ef_patch_migrering_vedtak" stored procedures',
    default_args = default_args,
    start_date = datetime(2023, 7, 25), # start date for the dag
    schedule_interval = '0 0 5 * *' , # 5te hver måned,
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

    patch_ybarn_arena = python_operator(
        dag=dag,
        name="fam_ef_patch_ybarn_infotrygd_arena",
        repo="navikt/team_familie_airflow_dags",
        script_path="Oracle_python/fam_ef_patch_ybarn.py",
        branch=branch,
        allowlist=allowlist,
        resources=client.V1ResourceRequirements(
            requests={"memory": "4G"},
            limits={"memory": "4G"}),
        slack_channel=Variable.get("slack_error_channel")
    )

    patch_migrerte_vedtak = python_operator(
        dag=dag,
        name="fam_ef_patch_migrert_vedtak",
        repo="navikt/team_familie_airflow_dags",
        script_path="Oracle_python/fam_ef_patch_migrerte_vedtak.py",
        branch=branch,
        allowlist=allowlist,
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
            message = "Patching av ybarn og migrerte_vedtak er nå ferdig kjørt! :tada: :tada:"
        )
    slutt_alert = notification_end()

start_alert >> patch_migrerte_vedtak >> patch_ybarn_arena >> slutt_alert


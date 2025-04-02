from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow import notebook_operator
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_info
from operators.dbt_operator import create_dbt_operator
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, sftp_ip

miljo = Variable.get('miljo')
allowlist = prod_oracle_conn_id + sftp_ip

#Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_bb_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]



with DAG(
  dag_id = 'BB_historisk_data_sftp_oracle',
  description = 'Leser barnbidrag fil fra sfto server, transformerer data og så gjøre insert til en tabell i Oracle database',
  start_date=datetime(2025, 4, 1),
  schedule_interval= '0 0 3 * *',
  max_active_runs=1,
  catchup = False
) as dag:

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(slack_allowlist)})
            )
        }
    )
    def notification_start():
        slack_info(
            message = f'Lesing av barnbidrag bis data fra SFTP serveren til Oracle i {miljo} database starter nå! :rocket:'
        )

    start_alert = notification_start()

    bb_bis_historisk_data = notebook_operator(
    dag = dag,
    name = 'BB_bis_historisk_data_kopiering',
    repo = 'navikt/dvh-fam-notebooks',
    nb_path = 'BB/bb_bis_en_periode.ipynb',
    allowlist=allowlist,
    branch = v_branch,
    resources=client.V1ResourceRequirements(
        requests={'memory': '4G'},
        limits={'memory': '4G'}),
    slack_channel = Variable.get('slack_error_channel'),
    requirements_path="requirements.txt",
    log_output=False
    )

    bb_berm_historisk_data = notebook_operator(
    dag = dag,
    name = 'BB_berm_historisk_data_kopiering',
    repo = 'navikt/dvh-fam-notebooks',
    nb_path = 'BB/bb_berm_en_periode.ipynb',
    allowlist=allowlist,
    branch = v_branch,
    resources=client.V1ResourceRequirements(
        requests={'memory': '4G'},
        limits={'memory': '4G'}),
    slack_channel = Variable.get('slack_error_channel'),
    requirements_path="requirements.txt",
    log_output=False
    )

    bb_historisk_data_wrangling = create_dbt_operator(
     dag=dag,
     name="historisk_bb_data_wrangling",
     repo='navikt/dvh_fam_bb_dbt',
     script_path = 'airflow/dbt_run.py',
     branch=v_branch,
     dbt_command= """run --select BB_historisk_data_trans.*""",
     db_schema=v_schema,
     allowlist=allowlist
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
            message = f'Historisk barnbidrag bis data fra SFTP til Oracle i {miljo} database er vellykket! :tada: :tada:'
        )
    slutt_alert = notification_end()

start_alert >> bb_bis_historisk_data >> bb_berm_historisk_data >> bb_historisk_data_wrangling >> slutt_alert



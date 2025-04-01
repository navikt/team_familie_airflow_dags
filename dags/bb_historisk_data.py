from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow import notebook_operator
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_info
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, sftp_ip

miljo = Variable.get('miljo')
branch = Variable.get("branch")
allowlist = prod_oracle_conn_id + sftp_ip



with DAG(
  dag_id = 'BB_historisk_data_sftp_oracle',
  description = 'Leser barnbidrag fil fra sfto server, transformerer data og så gjøre insert til en tabell i Oracle database',
  start_date=datetime(2025, 3, 28),
  schedule_interval= '0 0 2 * *',
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
            message = f'Lesing av barnbidrag data fra SFTP serveren til Oracle i {miljo} database starter nå! :rocket:'
        )

    start_alert = notification_start()

    bb_historisk_data = notebook_operator(
    dag = dag,
    name = 'BB_historisk_data_kopiering',
    repo = 'navikt/dvh-fam-notebooks',
    nb_path = 'BB/bb_bis_en_periode.ipynb',
    allowlist=allowlist,
    branch = branch,
    #delete_on_finish= False,
    resources=client.V1ResourceRequirements(
        requests={'memory': '4G'},
        limits={'memory': '4G'}),
    slack_channel = Variable.get('slack_error_channel'),
    requirements_path="requirements.txt",
    #image='ghcr.io/navikt/dvh_familie_image:2023-11-27-eccc5e8-main',
    log_output=False
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
            message = f'Historisk barnbidrag data fra SFTP til Oracle i {miljo} database er vellykket! :tada: :tada:'
        )
    slutt_alert = notification_end()

start_alert >> bb_historisk_data >> slutt_alert



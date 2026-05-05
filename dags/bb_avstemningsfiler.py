from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow import notebook_operator
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_info
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, sftp_ip

miljo = Variable.get('miljo')
allowlist = prod_oracle_conn_id + sftp_ip

#Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_bb_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

with DAG(
  dag_id = 'bb_avstemningsfiler',
  description = 'Leser barnbidrag avstemningsfiler fra bidrag filområde, transformerer data og så gjøre insert til avtsemnings tabeller i Oracle database',
  start_date=datetime(2026, 5, 5),
  schedule_interval= '0 5 1 * *',
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
            message = f'Lesing av barnbidrag avstemningsfiler fra SFTP serveren til Oracle i {miljo} database starter nå! :rocket:'
        )

    start_alert = notification_start()

    avstemningsfiler = notebook_operator(
    dag = dag,
    name = 'Barnebidrag_avstemningsfiler',
    repo = 'navikt/dvh-fam-notebooks',
    nb_path = 'BB/hent_avstemnings_filer.ipynb',
    allowlist=allowlist,
    branch = v_branch,
    resources=client.V1ResourceRequirements(
        requests={'memory': '4G'},
        limits={'memory': '4G'}),
    slack_channel = Variable.get('slack_error_channel'),
    requirements_path="requirements.txt",
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
            message = f'Lesing av barnbidrag avstemningsfiler fra SFTP til Oracle i {miljo} database er vellykket! :tada: :tada:'
        )
    slutt_alert = notification_end()

start_alert >> avstemningsfiler >> slutt_alert



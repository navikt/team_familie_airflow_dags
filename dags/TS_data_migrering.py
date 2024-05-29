from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow import notebook_operator
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_info
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, dev_oracle_conn_id,r_oracle_conn_id

miljo = Variable.get('miljo')
branch = Variable.get("branch")
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)

with DAG(
  dag_id = 'kopier_TS_data_fra_BigQuery_til_Oracle',
  description = 'kopierer tilleggsstonader data fra en tabell i BigQuery til en tabell i Oracle database',
  start_date=datetime(2024, 5, 29, 6), #06:00 om morgenen
  schedule_interval= '@daily',
  max_active_runs=1,
  catchup = True
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
            message = f'Kopiering av tilleggsstønader data fra BigQuery til Oracle i {miljo} database starter nå! :rocket:'
        )

    start_alert = notification_start()

    ts_data_kopiering = notebook_operator(
    dag = dag,
    name = 'TS_data_kopiering',
    repo = 'navikt/dvh-fam-notebooks',
    nb_path = 'TS/kopiere_ts_data_fra_bq_til_oracle.ipynb',
    allowlist=allowlist,
    branch = branch,
    #delete_on_finish= False,
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
            message = f'Kopiering av tilleggsstønader data fra BigQuery til Oracle i {miljo} database er vellykket! :tada: :tada:'
        )
    slutt_alert = notification_end()

start_alert >> ts_data_kopiering >> slutt_alert



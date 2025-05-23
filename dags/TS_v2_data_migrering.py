from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow import notebook_operator
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_info, slack_error
from operators.dbt_operator import create_dbt_operator
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, dev_oracle_conn_id,r_oracle_conn_id

miljo = Variable.get('miljo')
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]


with DAG(
  dag_id = 'kopier_TS_v2_data_fra_BigQuery_til_Oracle',
  description = 'kopierer tilleggsstonader data fra en tabell i BigQuery til en tabell i Oracle database',
  start_date=datetime(2025, 4, 28), 
  schedule_interval= '0 6 * * *', #06:00 om morgenen
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
            message = f'Kopiering av tilleggsstønader v2 data fra BigQuery til Oracle i {miljo} database starter nå! :rocket:'
        )

    start_alert = notification_start()
    
    # v2
    ts_data_kopiering_v2 = notebook_operator(
    dag = dag,
    name = 'TS_data_kopiering_v2',
    repo = 'navikt/dvh-fam-notebooks',
    nb_path = 'TS/kopiere_ts_data_v2_fra_bq_til_oracle.ipynb',
    allowlist=allowlist,
    branch = v_branch,
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
            message = f'Kopiering av tilleggsstønader v2 data fra BigQuery til Oracle i {miljo} database er vellykket! :tada: :tada:'
        )
    slutt_alert = notification_end()


    ts_utpakking_dbt = create_dbt_operator(
    dag=dag,
    name="utpakking_ts",
    repo='navikt/dvh_fam_ts_dbt',
    script_path = 'airflow/dbt_run.py',
    branch=v_branch,
    dbt_command= """run --select TS_utpakking_v2.*""",
    db_schema=v_schema,
    allowlist=allowlist
)

start_alert >> ts_data_kopiering_v2 >> ts_utpakking_dbt >> slutt_alert



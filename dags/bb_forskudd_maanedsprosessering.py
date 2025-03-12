from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from kubernetes import client
from felles_metoder.felles_metoder import get_periode, get_siste_dag_i_perioden
from allowlists.allowlist import slack_allowlist, dev_oracle_conn_id, prod_oracle_conn_id,r_oracle_conn_id

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
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("periode_variabler", deserialize_json=True)
v_periode_fom = settings["periode_fom"]
v_periode_fom = settings["periode_tom"]
v_max_vedtaksdato = settings["max_vedtaksdato"]
v_periode_type = settings["periode_type"]

periode_fom, periode_tom, max_vedtaksdato, periode_type  = None, None, None, None

if v_periode_fom is None:
    periode_fom = get_periode()
    periode_tom = get_periode()
    max_vedtaksdato = get_siste_dag_i_perioden()
    periode_type = 'M'
else:
    periode_fom = v_periode_fom
    periode_tom = v_periode_fom
    max_vedtaksdato = v_max_vedtaksdato
    periode_type = 'M'

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_bb_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

with DAG(
    dag_id = 'forskudd_maanedsprosessering', 
    description = 'An Airflow DAG to invoke dbt bb_forskudd_maanedsprosessering project to insert periode(s) into test_fak_bb_forskudd',
    default_args = default_args,
    start_date = datetime(2025, 3, 12), # start date for the dag
    schedule_interval = '0 0 1 * *' , #timedelta(days=1), schedule_interval='*/5 * * * *',
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
            message = f"Inserting perioden {periode_fom} til periode {periode_tom} med max_vedtaks_dato {max_vedtaksdato} av forskudd maanedsprosesserings data til test_fak_bb_forskudd starter nå! :rocket:"
        )

    start_alert = notification_start()

    dbt_run_stonad_arena = create_dbt_operator(
        dag=dag,
        name="dbt-run_forskudd_maanedsprosessering",
        repo='navikt/dvh_fam_bb_dbt',
        script_path = 'airflow/dbt_run.py',
        branch=v_branch,
        dbt_command=f"""run --select BB_maanedsprosessering.*  --vars '{{"periode_fom":{periode_fom}, "periode_tom":{periode_tom}, "max_vedtaksdato":{max_vedtaksdato}, "periode_type":{periode_type}}}' """,
        allowlist=allowlist, 
        db_schema=v_schema
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
            message = "Data er feridg lastet til test_fak_bb_forskudd! :tada: :tada:"
        )

    slutt_alert = notification_end()
   
start_alert >> dbt_run_stonad_arena >> slutt_alert

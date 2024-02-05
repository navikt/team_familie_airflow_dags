from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from kubernetes import client
from felles_metoder.felles_metoder import get_periode
from allowlists.allowlist import slack_allowlist, dev_oracle_conn_id, prod_oracle_conn_id

miljo = Variable.get('miljo')   
allowlist = []
if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

periode = get_periode() 

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = 'main'#settings["branch"]
v_schema = 'dvh_fam_ef'#settings["schema"]

with DAG(
    dag_id = 'Sas_erstatning', 
    description = 'An Airflow DAG to invoke dbt stonad_arena project and a Python script to insert into fam_ef_stonad_arena ',
    default_args = default_args,
    start_date = datetime(2022, 12, 1), # start date for the dag
    schedule_interval = '0 0 5 * *' , #timedelta(days=1), schedule_interval='*/5 * * * *',
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
            message = "Lasting av data til både fam_ef_arena_stonad og fam_ef_arena_vedtak starter nå ved hjelp av Airflow! :rocket:"
        )

    start_alert = notification_start()

    dbt_run_stonad_arena = create_dbt_operator(
        dag=dag,
        name="dbt-run_stonad_arena",
        script_path = 'airflow/dbt_run.py',
        branch=v_branch,
        dbt_command=f"""run --select EF_arena.*  --vars '{{"periode":{periode}}}' """,
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
            message = "Data er feridg lastet til fam_ef_arena_stonad og fam_ef_arena_vedtak! :tada: :tada:"
        )

    slutt_alert = notification_end()
   
start_alert >> dbt_run_stonad_arena >> slutt_alert

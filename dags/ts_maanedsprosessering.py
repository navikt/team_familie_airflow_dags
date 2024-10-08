from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from kubernetes import client
from felles_metoder.felles_metoder import get_periode
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
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

periode = get_periode() 

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

with DAG(
    dag_id = 'ts_maanedsprosessering',
    description = 'DAG som kjører insert i fag_ts_mottaker basert på periode', 
    default_args = default_args,
    start_date = datetime(2024, 10, 8), 
    schedule_interval = '0 0 5 * *' , # Den 5. hvert måned
    catchup = False 
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
            message = "Starter månedsprosessering av tilleggsstønader! :rocket:"
        )

    start_alert = notification_start()

    ts_dbt_insert = create_dbt_operator(
        dag=dag,
        name="insert_mottaker_data",
        script_path = 'airflow/dbt_run.py',
        branch=v_branch,
        dbt_command=f"""run --select TS_maanedsprosessering.*  --vars '{{"periode":{periode}}}' """,
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
            message = "Måndedsprosessering av tilleggsstønader fullført! :tada: :tada:" #TODO
        )

    slutt_alert = notification_end()
   
start_alert >> ts_dbt_insert >> slutt_alert

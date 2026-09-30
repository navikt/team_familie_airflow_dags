from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from kubernetes import client
from dataverk_airflow import notebook_operator
from felles_metoder.felles_metoder import get_periode, get_siste_dag_i_forrige_maaned
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, nb_api

miljo = Variable.get('miljo')

allowlist = prod_oracle_conn_id

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("bb_saerbidrag_variabler", deserialize_json=True)
v_periode = settings["periode"]
v_gyldig_flagg = settings["gyldig_flagg"]

periode, gyldig_flagg  = None, None

# Hvis veriabel er tom, sett til standard verdi, ellers bruk verdien fra variabelen
periode = get_periode() if v_periode == '' else v_periode
gyldig_flagg = 1 if v_gyldig_flagg == '' else v_gyldig_flagg

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_bb_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

with DAG(
    dag_id = 'saerbidrag_maanedsprosessering', 
    description = 'Automatiserer månedlig prosessering av særbidragsdata ved å kjøre dbt-prosjektet bb_saerbidrag_mnd og oppdatere relevante perioder i databasen.',
    default_args = default_args,
    start_date = datetime(2026, 9, 30), # start date for the dag
    schedule_interval = '0 0 2 * *' , #timedelta(days=1), schedule_interval='*/5 * * * *',
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
            message = f"Starter månedlig prosessering av særbidrag for {periode}. Periode={periode}, Gyldig flagg={gyldig_flagg} :rocket:"
        )

    start_alert = notification_start()

    

    saerbidrag_mnd = create_dbt_operator(
        dag=dag,
        name="dbt-run_saerbidrag_mnd",
        repo='navikt/dvh_fam_bb',
        script_path = 'airflow/dbt_run.py',
        branch=v_branch,
        dbt_command=f"""run --select BB_saerbidrag_mnd.*  --vars '{{"periode":{periode}, "gyldig_flagg":{gyldig_flagg}}}' """,
        allowlist=prod_oracle_conn_id, 
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
            message = "Data er ferdig lastet! :tada: :tada:"
        )

    slutt_alert = notification_end()
   
start_alert >> saerbidrag_mnd >> slutt_alert

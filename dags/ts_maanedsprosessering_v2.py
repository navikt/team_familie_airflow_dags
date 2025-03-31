from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from kubernetes import client
from felles_metoder.felles_metoder import get_periode
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, dev_oracle_conn_id, r_oracle_conn_id
from airflow.utils.log.logging_mixin import LoggingMixin

# Få tilgang til Airflows logger
logger = LoggingMixin().log

miljo = Variable.get('miljo')   
allowlist = []
if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)
    miljo = 'dev'

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

# Definerer DAG-nivå standardverdier for periode og gyldig_flagg
dag_params = {
    "periode": None,  # Standardverdi er tom, vil bli satt dynamisk hvis tom
    "gyldig_flagg": None  # Standardverdi er tom, vil bli satt dynamisk hvis tom
}

with DAG(
    dag_id='ts_maanedsprosessering_v2',
    description='DAG som kjører insert i fag_ts_mottaker basert på periode',
    default_args=default_args,
    start_date=datetime(2024, 10, 4),
    schedule_interval='*/15 * * * *',  # Hvert 15. minutt for test av mangel på manuell parameter for automatisk kjøring
    catchup=False,  # Endre til False hvis du ikke ønsker å kjøre oppsamlede kjøringer
    params=dag_params  # Legger til DAG-nivå parametere
) as dag:

    # Debugging: Log the parameters
    logger.info(f"dag.params: {dag.params}")

    # Setter periode og gyldig_flagg basert på dag.params eller fallback-logikk
    periode = dag.params.get("periode")
    if not periode:  # Sjekker om periode er None eller tom
        periode = get_periode()  # Standardverdi

    gyldig_flagg = dag.params.get("gyldig_flagg")
    if not gyldig_flagg:  # Sjekker om gyldig_flagg er None eller tom
        gyldig_flagg = 1  # Standardverdi
    else:
        gyldig_flagg = int(gyldig_flagg)  # Konverterer til heltall

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(slack_allowlist)})
            )
        }
    )
    def notification_start():
        slack_info(
            message=f"Starter månedsprosessering v2 {periode} med gyldig_flagg={gyldig_flagg} av tilleggsstønader for {miljo}! :rocket:"
        )

    start_alert = notification_start()

    ts_dbt_insert = create_dbt_operator(
        dag=dag,
        name="insert_mottaker_data",
        repo='navikt/dvh_fam_ts_dbt',
        script_path='airflow/dbt_run.py',
        branch=v_branch,
        dbt_command=f"""run --select TS_maanedsprosessering_v2.* --vars '{{"periode":"{periode}", "gyldig_flagg":{gyldig_flagg}}}' """,
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
            message=f"Måndedsprosessering v2 {periode} med gyldig_flagg={gyldig_flagg} av tilleggsstønader fullført! :tada: :tada:"
        )

    slutt_alert = notification_end()

start_alert >> ts_dbt_insert >> slutt_alert

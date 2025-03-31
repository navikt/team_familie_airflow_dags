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
settings = Variable.get("dbt_ef_schema_v2", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

with DAG(
    dag_id='ts_maanedsprosessering_v2',
    description='DAG som kjører insert i fag_ts_mottaker basert på periode',
    default_args=default_args,
    start_date=datetime(2024, 10, 4),
    schedule_interval='0 0 5 * *',  # Den 5. hvert måned
    catchup=False,  # Endre til False hvis du ikke ønsker å kjøre oppsamlede kjøringer
    # Legger til DAG-nivå parametere
    params={ 
        "periode": None,  # Standardverdi er tom, vil bli satt dynamisk hvis tom
        "gyldig_flagg": None  # Standardverdi er tom, vil bli satt dynamisk hvis tom
    },   
) as dag:

    # Debugging: Logg parametere
    logger.info(f"Full dag.params: {dag.params}")
    logger.info(f"Parameter 'periode' mottatt: {dag.params.get('periode')}")
    logger.info(f"Parameter 'gyldig_flagg' mottatt: {dag.params.get('gyldig_flagg')}")

    # Setter periode og gyldig_flagg basert på dag.params eller fallback-logikk
    periode = dag.params.get("periode")
    if not periode:  # Sjekker om periode er None eller tom
        logger.info("Parameter 'periode' er ikke satt eller tom. Bruker fallback til get_periode().")
        periode = get_periode()  # Standardverdi

    gyldig_flagg = dag.params.get("gyldig_flagg")
    if not gyldig_flagg:  # Sjekker om gyldig_flagg er None eller tom
        logger.info("Parameter 'gyldig_flagg' er ikke satt eller tom. Bruker fallback til 1.")
        gyldig_flagg = 1  # Standardverdi
    else:
        try:
            gyldig_flagg = int(gyldig_flagg)  # Konverterer til heltall
        except ValueError:
            logger.error(f"Ugyldig verdi for gyldig_flagg: {gyldig_flagg}. Bruker fallback til 1.")
            gyldig_flagg = 1

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

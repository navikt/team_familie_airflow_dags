from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from kubernetes import client
from felles_metoder.felles_metoder import get_periode
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, dev_oracle_conn_id, r_oracle_conn_id
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.param import Param

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

# Oppgave for å lese og bruke parametere. Se følgende dokumentasjon under for hvorfor:
"""DAG-level parameters are the default values passed on to tasks. These should not be confused with values manually provided through the UI form or CLI, 
which exist solely within the context of a DagRun and a TaskInstance. This distinction is crucial for TaskFlow DAGs, which may include logic within the with DAG(...) as dag: block. 
In such cases, users might try to access the manually-provided parameter values using the dag object, but this will only ever contain the default values. 
To ensure that the manually-provided values are accessed, use a template variable such as params or ti within your task.
https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html"""
@task
def process_params(params):
    # Les parametere fra TaskInstance (ikke dag.params)
    periode = params.get("periode")
    if not periode:  # Sjekker om periode er None eller tom
        logger.info("Parameter 'periode' er ikke satt eller tom. Bruker fallback til get_periode().")
        periode = get_periode()  # Standardverdi
    else:
        logger.info(f"Parameter 'periode' satt til: {periode}")

    gyldig_flagg = params.get("gyldig_flagg")
    try:
        # Konverterer til heltall hvis gyldig_flagg er satt
        gyldig_flagg = int(gyldig_flagg) if gyldig_flagg is not None else 1
        if gyldig_flagg != 1:
            logger.info(f"Parameter 'gyldig_flagg' satt til: {gyldig_flagg}")
        else:
            logger.info("Parameter 'gyldig_flagg' er standardverdi 1.")
    except ValueError:
        logger.error(f"Ugyldig verdi for gyldig_flagg: {gyldig_flagg}. Bruker fallback til 1.")
        gyldig_flagg = 1

    return periode, gyldig_flagg

with DAG(
    dag_id='ts_maanedsprosessering_v2_helen',
    description='DAG som kjører insert i fag_ts_mottaker basert på periode',
    default_args=default_args,
    start_date=datetime(2025, 3, 31),
    schedule_interval=None,  # Den 5. hvert måned
    catchup=False,  # Endre til False hvis du ikke ønsker å kjøre oppsamlede kjøringer
    params={"periode": Param(6, type="integer"), "gyldig_flagg": Param(2, type="integer")}
) as dag:


    # Task-level params take precedence over DAG-level params, and user-supplied params (when triggering the DAG) take precedence over task-level params.
    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(slack_allowlist)})
            )
        }
    )
    def show_params(**kwargs) -> None:
        params: ParamsDict = kwargs["params"]
        print("This DAG was triggered with the following parameters:"+params['periode'])

    show_params=show_params()

    ts_dbt_insert = create_dbt_operator(
        dag=dag,
        name="insert_mottaker_data",
        repo='navikt/dvh_fam_ts_dbt',
        script_path='airflow/dbt_run.py',
        branch=v_branch,
        dbt_command=f"""run --select TS_maanedsprosessering_v2.* --vars '{{"periode": params["periode"], "gyldig_flagg": params["gyldig_flagg"]}}' """,
        allowlist=allowlist,
        db_schema=v_schema
    )

    show_params >> ts_dbt_insert
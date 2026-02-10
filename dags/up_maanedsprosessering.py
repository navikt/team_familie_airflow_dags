from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG, Variable
from airflow.decorators import task
from kubernetes import client
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id


def get_or_default(value, fallback, cast=str):
    """Returner verdi hvis satt, ellers fallback()."""
    if value in ("", None):
        return fallback()
    return cast(value)


# Hent alle miljøvariabler og konfigurasjon
miljo = Variable.get("miljo")

OSLO_TZ = pendulum.timezone("Europe/Oslo")
now_oslo = pendulum.now(OSLO_TZ)
tomorrow = now_oslo.add(days=1).replace(microsecond=0)

up_variabler = Variable.get("up_variabler", deserialize_json=True)
periode_fom      = get_or_default(up_variabler["periode_fom"], None) # Fallback skal være None, da håndteres det i intermediate-delen av mndprosesseringen 
periode_tom      = get_or_default(up_variabler["periode_tom"], None)
max_vedtaksdato  = get_or_default(up_variabler["max_vedtaksdato"], tomorrow) # For UP skal være dagens dato + 1
#periode_type     = get_or_default(up_variabler["periode_type"], lambda: "M")
gyldig_flagg     = get_or_default(up_variabler["gyldig_flagg"], lambda: 1, cast=int)

dbt_settings = Variable.get("dbt_up_schema", deserialize_json=True)
v_branch = dbt_settings["branch"]
v_schema = dbt_settings["schema"]

# DAG-konfigurasjoner
default_args = {
    "owner": "Team-Familie",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": slack_error,
}

pod_slack_allowlist = {
    "executor_config": {
        "pod_override": client.V1Pod(
            metadata=client.V1ObjectMeta(
                annotations={"allowlist": ",".join(slack_allowlist)}
            )
        )
    }
}


with DAG(
    dag_id="up_maanedsprosessering",
    description="Kjører månedlig prosessering og DBT-modeller for UP.",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 10, tz=OSLO_TZ),
    schedule_interval="0 0 17 * *", # Kjører hver dag kl. 17:00
    catchup=False,
) as dag:


    # ** pakker ut dictionaryen pod_slack_allowlist, sendes som argumenter til @task-dekoratøren. Prøver å unngå repiterende kode
    @task(**pod_slack_allowlist)
    def notification_start():
        if periode_fom == periode_tom: # Velger melding som passer grammatisk med perioder gitt som variabel
            periode_status = f"perioden {periode_fom}"
        else:
            periode_status = f"periodene {periode_fom} til og med {periode_tom}"
        
        slack_info(
            message=(
                f"Starter månedlig prosessering for {periode_status}. Max vedtaksdato={max_vedtaksdato}, gyldig_flagg={gyldig_flagg} :rocket:"
            )
        )

    start_alert = notification_start()

    # Hentet ut av dbt_command for å øke lesbarheten 
    dbt_vars = (
        '{'
        f'"periode_fra": "{periode_fom}", '
        f'"periode_til": "{periode_tom}", '
        f'"max_vedtaksdato": "{max_vedtaksdato}", '
        #f'"periode_type": "{periode_type}", ' # Ikke implementert ennå, kommentert ut
        f'"gyldig_flagg": {gyldig_flagg}'
        '}'
    )

    dbt_run = create_dbt_operator(
        dag=dag,
        name="dbt_run_maanedsprosessering_up",
        repo="navikt/dvh_fam_ungdom",
        script_path="airflow/dbt_run.py",
        branch=v_branch,
        dbt_command=f"run --select UP_maanedsprosessering.* --vars '{dbt_vars}'",
        allowlist=prod_oracle_conn_id,
        db_schema=v_schema,
    )

    @task(**pod_slack_allowlist)
    def notification_end():
        slack_info(message="Månedlig prosessering av Ungdomsprogram er ferdig! :tada:")

    end_alert = notification_end()

    start_alert >> dbt_run >> end_alert
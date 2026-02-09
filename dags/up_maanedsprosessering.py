from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.decorators import task
from kubernetes import client
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from felles_metoder.felles_metoder import get_periode, get_siste_dag_i_forrige_maaned
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id


def get_or_default(value, fallback, cast=str):
    """Returner verdi hvis satt, ellers fallback()."""
    if value in ("", None):
        return fallback()
    return cast(value)


# Hent alle miljøvariabler og konfigurasjon
miljo = Variable.get("miljo")

up_variabler = Variable.get("up_variabler", deserialize_json=True)
periode_fom      = get_or_default(up_variabler["periode_fom"], get_periode)
periode_tom      = get_or_default(up_variabler["periode_tom"], get_periode)
max_vedtaksdato  = get_or_default(up_variabler["max_vedtaksdato"], get_siste_dag_i_forrige_maaned) # Skal være dagens dato + 1
periode_type     = get_or_default(up_variabler["periode_type"], lambda: "M")
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
    start_date=datetime(2025, 1, 1),  # TODO: sett start
    schedule_interval="0 0 17 * *", # Kjører hver dag kl. 17:00
    catchup=False,
) as dag:

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

    # Hentet ut av dbt_command for økt leselighet
    dbt_vars = (
        '{'
        f'"periode_fom": "{periode_fom}", '
        f'"periode_tom": "{periode_tom}", '
        f'"max_vedtaksdato": "{max_vedtaksdato}", '
        f'"periode_type": "{periode_type}", '
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
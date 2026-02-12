from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG, Variable
from airflow.decorators import task
from kubernetes import client
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id


def get_or_default(value, fallback, cast=str):
    """
    Returner cast(value) hvis value er satt (ikke "" eller None).
    Ellers:
      - hvis fallback er kallbar: returner fallback()
      - ellers: returner fallback (som verdi)
    """
    if value in ("", None):
        return fallback() if callable(fallback) else fallback
    return cast(value)


# Hent alle miljøvariabler og konfigurasjon
miljo = Variable.get("miljo")

OSLO_TZ = pendulum.timezone("Europe/Oslo")
now_oslo = pendulum.now(OSLO_TZ)
default_max_vedtaksdato = now_oslo.add(days=1).format("YYYYMMDD")
# Defaults tilsvarende intermediate-logikken, gjør ikke noe at det settes begge steder. Betyr at hvis man kjører DBT lokalt vil du fortsatt ha default der også
default_periode_fom = now_oslo.subtract(months=5).format("YYYYMM")
default_periode_tom = now_oslo.add(months=14).format("YYYYMM")

up_variabler = Variable.get("up_variabler", deserialize_json=True)
periode_fom      = get_or_default(up_variabler.get("periode_fra"), lambda: default_periode_fom)
periode_tom      = get_or_default(up_variabler.get("periode_til"), lambda: default_periode_tom)
max_vedtaksdato  = get_or_default(up_variabler.get("max_vedtaksdato"), lambda: default_max_vedtaksdato) # For UP skal være dagens dato + 1
# Periode_type:
# D = Daglig (default)
# M = Månedlig
# K = Kvartal
# H = Halvår
# A = År
periode_type     = get_or_default(up_variabler.get("periode_type"), lambda: 'D')
gyldig_flagg     = get_or_default(up_variabler.get("gyldig_flagg"), 1, cast=int)

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
    description="Kjører dagelig månedsprosessering for Ungdomsprogrammet.",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 12, tz=OSLO_TZ),
    schedule_interval="0 17 * * *", # Kjører hver dag kl. 17:00 CET
    catchup=False,
) as dag:


    # ** pakker ut dictionaryen pod_slack_allowlist, sendes som argumenter til @task-dekoratøren. Forsøker å abstrahere kode for renere lesbarhet
    @task(**pod_slack_allowlist)
    def notification_start():
        if periode_fom == periode_tom: # Velger melding som passer grammatisk med antall perioder gitt som variabel
            periode_status = f"perioden {periode_fom}"
        else:
            periode_status = f"periodene {periode_fom} til og med {periode_tom}"
        
        slack_info(
            message=(
                f"Starter dagelig månedsprosessering for {periode_status}. Max vedtaksdato={max_vedtaksdato}, periode_type={periode_type}, gyldig_flagg={gyldig_flagg} :rocket:"
            )
        )

    start_alert = notification_start()

    dbt_run = create_dbt_operator(
        dag=dag,
        name="dbt_run_maanedsprosessering_up",
        repo="navikt/dvh_fam_ungdom",
        script_path="airflow/dbt_run.py",
        branch=v_branch,
        dbt_command=f"""run --select UP_maanedsprosessering.* --vars '{{"periode_fra":{periode_fom}, "periode_til":{periode_tom}, "max_vedtaksdato":{max_vedtaksdato}, "periode_type":{periode_type},"gyldig_flagg":{gyldig_flagg}}}'""",
        allowlist=prod_oracle_conn_id,
        db_schema=v_schema,
    )

    @task(**pod_slack_allowlist)
    def notification_end():
        slack_info(message="Daglig månedsprosessering av Ungdomsprogram er ferdig! :tada:")

    end_alert = notification_end()

    start_alert >> dbt_run >> end_alert
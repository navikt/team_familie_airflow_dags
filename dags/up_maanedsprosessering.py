from datetime import timedelta
import json
import pendulum

from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from kubernetes import client

from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_info, slack_error
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id


def detect_period_type(now: pendulum.DateTime) -> str | None:
    """
    Returnerer periodetype basert på dagens dato. Returnerer None hvis måneden ikke matcher med noe. 
    - 1. jan  -> A, Årskjøring
    - 1. apr  -> K, Kvartalskjøring
    - 1. jul  -> H, Halvårskjøring
    - 1. okt  -> S (ukjent betydning)
    """
    day = now.day
    month = now.month

    if day == 1 and month == 1:
        return "A"
    if day == 1 and month == 4:
        return "K"
    if day == 1 and month == 7:
        return "H"
    if day == 1 and month == 10:
        return "S"

    return None

def initialize_variables(periode_type: str, now_oslo: pendulum.DateTime, forskyvningsdager: int) -> tuple[str, str, str, str]:
    """
    Returnerer FOM, TOM, MAX_VEDTAKSDATO og ferdig periode_type-format basert på parametere. 
    """
    max_vedtaksdato = now_oslo.add(days=forskyvningsdager).format("YYYYMMDD")
    periode_tom = now_oslo.subtract(months=1).format("YYYYMM") # Alltid det samme, uansett periode_type. Kan derfor hardkodes
    periode_fom = None

    if periode_type == "A":
        periode_fom = now_oslo.subtract(months=12).format("YYYYMM")
        year = periode_fom.format("YYYY")
        periode_type = f"A{year}"

    elif periode_type == "S":
        periode_fom = now_oslo.subtract(months=9).format("YYYYMM")
        periode_type = f"S{periode_tom}"

    elif periode_type == "H":
        periode_fom = now_oslo.subtract(months=6).format("YYYYMM")
        periode_type = f"H{periode_fom}"

    elif periode_type == "K":
        periode_fom = now_oslo.subtract(months=3).format("YYYYMM")
        periode_type = f"K{periode_fom}"

    return periode_fom, periode_tom, max_vedtaksdato, periode_type


# Henter alle miljøvariabler
miljo = Variable.get("miljo")
OSLO_TZ = pendulum.timezone("Europe/Oslo")
now_oslo = pendulum.now(OSLO_TZ)

# Henter parametere
dbt_settings = Variable.get("dbt_up_schema", deserialize_json=True)
v_branch = dbt_settings["branch"]
v_schema = dbt_settings["schema"]

up_variabler = Variable.get("up_variabler_mnd", deserialize_json=True)
forskyvningsdager = up_variabler.get("forskyvningsdager") or 1 # Default hardkodet
gyldig_flagg = up_variabler.get("gyldig_flagg") or 1

# Finn periode_type basert på dagens dato. Blir None om ikke matcher med noen ønskede datoer
periode_type = detect_period_type(now_oslo)
periode_fom, periode_tom, max_vedtaksdato, periode_type = initialize_variables(
    periode_type,
    now_oslo,
    forskyvningsdager
)


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
    description="Kjører automatisk prosessering for Ungdomsprogrammet.",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 1, tz=OSLO_TZ),
    schedule_interval="0 0 1 */3 *", # 1. dag i hver 3. måned (jan, apr, jul, okt)
    catchup=False,
) as dag:

    @task.short_circuit
    def should_run() -> bool:
        """
        Stopper DAG-kjøring hvis dato ikke matcher periodetype. Dette er for å forhindre at månedsprosesseringen ikke kjører på feil tidspunkt, da tidspunktene må være nøyaktige
        """
        periode_type = detect_period_type(now_oslo) # Kjører først en gang bare for å identifisere om dato passer
        if periode_type is None:
            slack_info(
                message=(f"Denne DAGen skal kun kjøre 1. jan., 1. apr., 1. jul. eller 1. okt. Skipper derfor DAG! :tada:")
            )
            return False
        return True   

    # ** pakker ut dictionaryen pod_slack_allowlist, sendes som argumenter til @task-dekoratøren. Forsøker å abstrahere kode for renere lesbarhet
    @task(**pod_slack_allowlist)
    def notification_start():
        slack_info(
            message=(f"Starter månedelig prosessering for Ungdomsprogrammet. Periode_type={periode_type}. FOM={periode_fom}, TOM={periode_tom}. Max vedtaksdato={max_vedtaksdato}, antall forskyvningsdager={forskyvningsdager}. Gyldig_flagg={gyldig_flagg}. :rocket:")
        )

    start_alert = notification_start()

    dbt_run = create_dbt_operator(
        dag=dag,
        name="dbt_run_månedelig_prosessering_up",
        repo="navikt/dvh_fam_ungdom",
        script_path="airflow/dbt_run.py",
        branch=v_branch,
        dbt_command=f"""run --select UP_maanedsprosessering.* --vars '{{"periode_fra":{periode_fom}, "periode_til":{periode_tom}, "max_vedtaksdato":{max_vedtaksdato}, "periode_type":{periode_type},"gyldig_flagg":{gyldig_flagg}}}'""",
        allowlist=prod_oracle_conn_id,
        db_schema=v_schema,
    )

    @task(**pod_slack_allowlist)
    def notification_end():
        slack_info(
            message=(f"Månedsprosessering av Ungdomsprogrammet er ferdig! :tada:")
        )

    end_alert = notification_end()

    should_run() >> start_alert >> dbt_run >> end_alert
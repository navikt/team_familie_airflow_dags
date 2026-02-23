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
    - 1. okt  -> S
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


def compute_period_bounds(
    periode_type: str,
    now_oslo: pendulum.DateTime,
    forskyvningsdager: int
) -> tuple[str, str, str, str]:
    """
    Beregner FOM, TOM, MAX_VEDTAKSDATO og ferdig periode_type-format.
    """
    max_vedtaksdato = now_oslo.add(days=forskyvningsdager).format("YYYYMMDD")
    periode_tom = now_oslo.subtract(months=1).format("YYYYMM")
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
up_variabler_mnd = Variable.get("up_variabler_mnd", deserialize_json=True)

DEFAULT_FORSKYVNING = 1
DEFAULT_GYLDIG_FLAGG = 1

forskyvningsdager = up_variabler_mnd.get("forskyvningsdager") or DEFAULT_FORSKYVNING
gyldig_flagg = up_variabler_mnd.get("gyldig_flagg") or DEFAULT_GYLDIG_FLAGG

# Finn periode_type basert på dagens dato. Blir None om ikke matcher med noen ønskede datoer.
periode_type = detect_period_type(now_oslo)

if periode_type is None:
    raise AirflowSkipException(
        f"Ingen periodetype treffer i dag. Denne DAGen kjører kun 1. jan., 1. apr., 1. jul. eller 1. okt. Skipper DAG! :tada:"
    )

periode_fom, periode_tom, max_vedtaksdato, periode_type = compute_period_bounds(
    periode_type,
    now_oslo,
    forskyvningsdager
)

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
    description="Kjører automatisk prosessering for Ungdomsprogrammet.",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 1, tz=OSLO_TZ),
    schedule_interval="0 1 * * *", # Like etter midnatt, så vi er sikre på at det skjer riktig dag
    catchup=False,
) as dag:


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

    start_alert >> dbt_run >> end_alert
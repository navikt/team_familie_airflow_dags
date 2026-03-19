import pendulum
from datetime import datetime
from operators.dbt_operator import create_dbt_operator
from airflow.models import DAG
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_info
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id,dbt_docs_nav_server


allowlist = prod_oracle_conn_id + dbt_docs_nav_server

OSLO = pendulum.timezone("Europe/Oslo")

with DAG(
    dag_id="dbt_docs",
    start_date=datetime(2026, 3, 18, tzinfo=OSLO),  
    schedule="0 0 * * 1",   
    tags=["docs", "dbt"],
    catchup=False,
    max_active_runs=1, 
) as dag:

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(slack_allowlist)})
            )
        }
    )
    def notification_start():
        slack_info(
            message = 'Genererer dbt-docs for alle dbt prosjektene våre! :rocket:'
        )

    start_alert = notification_start()

    repos = [
        # navn,       skjema,          repo,                   path_til_dbt_run_i_repo
        ("bt", "dvh_fam_bt",      "navikt/dvh_fam_bt",      "airflow/dbt_run.py"),
        ("ks", "dvh_fam_ks",      "navikt/dvh_fam_ks",      "airflow/dbt_run.py"),
        #("ef", "dvh_fam_ef",      "navikt/dvh_fam_ef",      "airflow/dbt_run.py"),
        #("pp", "dvh_fam_pp",      "navikt/dvh_fam_pp",      "airflow/dbt_run.py"),
        ("fp", "dvh_fam_fp",      "navikt/dvh_fam_fp",      "airflow/dbt_run.py"),
        #("bb", "dvh_fam_bb",      "navikt/dvh_fam_bb",      "airflow/dbt_run.py"),
        #("up", "dvh_fam_ungdom",  "navikt/dvh_fam_ungdom",  "airflow/dbt_run.py"),
    ]

    tasks = []
    for name, schema, repo, script_path in repos:
        t = create_dbt_operator(
            dag=dag,
            name=f"{name}_dbt_docs_generate",
            retries=1,  
            branch="main",
            dbt_command="docs generate",
            db_schema=schema,
            repo=repo,
            script_path=script_path,
            allowlist=allowlist,
            publish_docs=True,
        )
        tasks.append(t)

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(slack_allowlist)})
            )
        }
    )
    def notification_end():
        slack_info(
            message = f'Generering av dbt-docs for projektene våre er vellykket! :tada: :tada:'
        )
    slutt_alert = notification_end()

    start_alert >> tasks >> slutt_alert
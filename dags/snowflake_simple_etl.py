import json
import logging
import os


SNOWFLAKE = [
    "QQHHRQV-DV_TEAM_FAMILIE.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
 
 
def main():
    import snowflake.connector
 
    logging.basicConfig(level=logging.INFO)
    logging.info("Hello, world! Testing the Snowflake GCP WIF connection.")
    config = json.loads(os.environ["SNOW_CONN"])
    with snowflake.connector.connect(
        account=config["account"],
        authenticator="WORKLOAD_IDENTITY",
        workload_identity_provider="GCP",
        role=config.get("role"),
        warehouse=config.get("warehouse"),
        login_timeout=30,
        network_timeout=30,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1", timeout=30)
            result = cursor.fetchone()
            if result != (1,):
                raise ValueError(f"Expected SELECT 1 to return (1,), got {result!r}")
            logging.info("Snowflake GCP WIF connection verified: SELECT 1 returned 1")
 
 
if __name__ == "__main__":
    main()
else:
    from datetime import datetime
 
    from airflow import DAG
    from airflow.models import Variable
    from dataverk_airflow import python_operator
 
    #from orchestrator.operators.allowlists import SNOWFLAKE
 
    with DAG(
        "test_snowflake_wif",
        start_date=datetime(2026, 11, 24),
        schedule_interval=None,
        catchup=False,
        default_args={"retries": 0},
    ) as dag:
        python_operator(
            dag=dag,
            name="select_one",
            repo="navikt/vdl-regnskapsdata",
            branch='dev'#Variable.get("git_branch"),
            script_path="dags/test_snowflake_wif.py",
            requirements_path="orchestrator/snowflake_wif/requirements.txt",
            slack_channel=Variable.get("slack_error_channel"),
            extra_envs={
                "PYTHONUNBUFFERED": "1",
                "SNOW_CONN": json.dumps(
                    Variable.get("conn_snowflake_wif", deserialize_json=True)
                ),
            },
            allowlist=SNOWFLAKE.copy(),
            python_version="3.13",
            use_uv_pip_install=True,
            retries=0,
        )
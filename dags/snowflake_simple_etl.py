import logging
from datetime import datetime
 
from airflow.decorators import dag, task
from kubernetes import client as k8s
 
#from orchestrator.operators.allowlists import SNOWFLAKE


SNOWFLAKE = [
    "wx23413.europe-west4.gcp.snowflakecomputing.com",
    "ocsp.snowflakecomputing.com",
    "ocsp.digicert.com:80",
    "o.pki.goog:80",
    "ocsp.pki.goo:80",
    "storage.googleapis.com",
]
 
 
@dag(
    start_date=datetime(2026, 11, 24),
    schedule_interval=None,
    catchup=False,
    default_args={
        "retries": 0,
        "executor_config": {
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={"allowlist": ",".join(SNOWFLAKE)}
                ),
                spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]),
            )
        },
    },
)
def test_snowflake_wif():
    @task
    def select_one():
        import snowflake.connector
        from airflow.models import Variable
 
        config = Variable.get("conn_snowflake_wif", deserialize_json=True)
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
                    raise ValueError(
                        f"Expected SELECT 1 to return (1,), got {result!r}"
                    )
                logging.info(
                    "Snowflake GCP WIF connection verified: SELECT 1 returned 1"
                )
 
    select_one()
 
 
dag = test_snowflake_wif()
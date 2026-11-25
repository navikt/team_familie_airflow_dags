from datetime import datetime
 
from airflow.decorators import dag, task
from kubernetes import client as k8s
 
 
@dag(
    start_date=datetime(2026, 11, 24),
    schedule_interval=None,
    catchup=False,
    default_args={
        "executor_config": {
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    annotations={
                        "allowlist": "34.141.231.182"
                    }
                ),
                spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]),
            )
        }
    },
)
def test_wif_auth():
    @task
    def select_one_sf():
        import snowflake.connector
        from airflow.models import Variable
 
        config = Variable.get("conn_snowflake_wif", deserialize_json=True)
        with snowflake.connector.connect(
            account=config["account"],
            user=config["user"],
            authenticator="WORKLOAD_IDENTITY",
            workload_identity_provider="GCP",
            login_timeout=30,
            network_timeout=30,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1", timeout=30)
 
                print(cursor.fetchone())
 
    select_one_sf()
 
 
test_wif_auth()
from datetime import datetime

from airflow import DAG
#from airflow.datasets import Dataset
#from airflow.models import Variable
from dataverk_airflow import python_operator

#snowflake_conn = Variable.get("conn_snowflake_wif", deserialize_json=True)

# SNOW_ALLOW = [    
#     "QQHHRQV-DV_TEAM_FAMILIE.snowflakecomputing.com",
#     "ocsp.snowflakecomputing.com",
#     "ocsp.digicert.com:80",
#     "o.pki.goog:80",
#     "ocsp.pki.goo:80",
#     "storage.googleapis.com",]


with DAG(
    dag_id="snow_hello_world",
    start_date=datetime(2026, 11, 25),
    catchup=False,
) as dag:

    hello = python_operator(
        dag=dag,
        name="hello_world",
        repo="navikt/team_familie_airflow_dags",
        branch="dev",
        script_path="snow_script.py",
        requirements_path="requirements.txt",
        #extra_envs={},
        #allowlist=SNOW_ALLOW,
        use_uv_pip_install=True,
        python_version="3.13",
    )

    hello

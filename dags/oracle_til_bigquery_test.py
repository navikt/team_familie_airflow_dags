from airflow import DAG
from datetime import datetime
from felles_metoder.felles_metoder import oracle_to_bigquery
from utils.db.oracle_conn import oracle_conn

with DAG('SimpleOracleToBigqueryOperator', start_date=datetime(2023, 2, 14), schedule=None) as dag:
    ekstrainfo = oracle_conn()

    oracle_to_bq = oracle_to_bigquery(
        oracle_con_id="oracle_con",
        oracle_table="FAM_ORACLE_BIGQUERY_TEST",
        gcp_con_id="google_con_different_project",
        bigquery_dest_uri="dv-familie-dev-f48b.test.fra_oracle_fp",
    )

    oracle_to_bq
    ekstrainfo
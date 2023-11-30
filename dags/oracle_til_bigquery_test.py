from airflow import DAG
from datetime import datetime
from felles_metoder import oracle_to_bigquery

with DAG('SimpleOracleToBigqueryOperator', start_date=datetime(2023, 2, 14), schedule=None) as dag:
    oracle_to_bq = oracle_to_bigquery(
        oracle_con_id="oracle_con",
        oracle_table="FAM_ORACLE_BIGQUERY_TEST",
        gcp_con_id="google_con_different_project",
        bigquery_dest_uri="dv-familie-dev-f48b.test.fra_oracle_fp",
    )

    oracle_to_bq
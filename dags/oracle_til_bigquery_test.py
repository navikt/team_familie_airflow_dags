from airflow import DAG
from datetime import datetime
from felles_metoder.felles_metoder import oracle_to_bigquery
from felles_metoder.felles_metoder import oracle_secrets

with DAG('SimpleOracleToBigqueryOperator', start_date=datetime(2023, 2, 14), schedule=None) as dag:

    v_oracle_con = oracle_secrets()

    oracle_to_bq = oracle_to_bigquery(
        oracle_con_id=v_oracle_con,#"oracle_con",
        oracle_table="DVH_FAM_FP.FAM_ORACLE_BIGQUERY_TEST",
        gcp_con_id="google_con_different_project",
        bigquery_dest_uri="dv-familie-dev-f48b.test.fra_oracle_fp",
    )

    oracle_to_bq

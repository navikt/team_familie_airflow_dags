from airflow import DAG
from datetime import datetime
from felles_metoder.felles_metoder import oracle_to_bigquery
from felles_metoder.felles_metoder import oracle_secrets
import cx_Oracle
import oracledb

with DAG('SimpleOracleToBigqueryOperator', start_date=datetime(2023, 2, 14), schedule=None) as dag:

    oracle_secrets = oracle_secrets()
    user = oracle_secrets['user'] + '[DVH_FAM_FP]'
    dsn_tns = cx_Oracle.makedsn(oracle_secrets['host'], 1521, service_name = oracle_secrets['service'])

    oracle_to_bq = oracle_to_bigquery(
        oracle_con_id=oracledb.connect(user=user, password = oracle_secrets['password'], dsn=dsn_tns),#"oracle_con",
        oracle_table="DVH_FAM_FP.FAM_ORACLE_BIGQUERY_TEST",
        gcp_con_id="google_con_different_project",
        bigquery_dest_uri="dv-familie-dev-f48b.test.fra_oracle_fp",
    )
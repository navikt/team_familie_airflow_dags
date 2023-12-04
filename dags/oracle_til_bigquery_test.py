from airflow import DAG
from datetime import datetime
from felles_metoder.felles_metoder import oracle_to_bigquery
from felles_metoder.felles_metoder import oracle_secrets
import cx_Oracle

with DAG('SimpleOracleToBigqueryOperator', start_date=datetime(2023, 2, 14), schedule=None) as dag:

    secrets = oracle_secrets()
    dsn_tns = cx_Oracle.makedsn(secrets['host'], 1521, service_name = secrets['service'])

    with cx_Oracle.connect(user = secrets['user']+'[dvh_fam_fp]', password = secrets['password'], dsn = dsn_tns) as connection:
        with connection.cursor() as cursor:

            cursor.oracle_to_bq = oracle_to_bigquery(
                oracle_con_id=dsn_tns,#"oracle_con",
                oracle_table="DVH_FAM_FP.FAM_ORACLE_BIGQUERY_TEST",
                gcp_con_id="google_con_different_project",
                bigquery_dest_uri="dv-familie-dev-f48b.test.fra_oracle_fp",
            )
            
            connection.commit()

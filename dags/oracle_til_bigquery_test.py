import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from datetime import datetime


def oracle_to_bigquery(
    oracle_con_id: str,
    oracle_table: str,
    gcp_con_id: str,
    bigquery_dest_uri: str,
    columns: list = [],
):
    columns = ",".join(columns) if len(columns) > 0 else "*"
    write_disposition = "WRITE_TRUNCATE"
    sql=f"SELECT {columns} FROM {oracle_table}"

    oracle_to_bucket = OracleToGCSOperator(
        task_id="oracle-to-bucket",
        oracle_conn_id=oracle_con_id,
        gcp_conn_id=gcp_con_id,
        impersonation_chain=f"{os.getenv('TEAM')}@knada-gcp.iam.gserviceaccount.com",
        sql=sql,
        bucket=os.getenv("AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER").removeprefix("gs://"),
        filename=oracle_table,
        export_format="csv"
    )

    bucket_to_bq = GCSToBigQueryOperator(
        task_id="bucket-to-bq",
        bucket="ORACLE_BQ_TEST_APEN_DATA",
        gcp_conn_id=gcp_con_id,
        destination_project_dataset_table=bigquery_dest_uri,
        impersonation_chain="TEAM-FAMILIE-DEV-ZWEX@knada-gcp.iam.gserviceaccount.com",
        autodetect=True,
        write_disposition=write_disposition,
        source_objects=oracle_table,
        source_format="csv"
    )

    delete_from_bucket = GoogleCloudStorageDeleteOperator(
        task_id="delete-from-bucket",
        bucket_name="ORACLE_BQ_TEST_APEN_DATA",
        objects=[oracle_table],
        gcp_conn_id=gcp_con_id,
    )

    return oracle_to_bucket >> bucket_to_bq >> delete_from_bucket


with DAG('SimpleOracleToBigqueryOperator', start_date=datetime(2023, 2, 14), schedule=None) as dag:
    oracle_to_bq = oracle_to_bigquery(
        oracle_con_id="oracle_con",
        oracle_table="FAM_ORACLE_BIGQUERY_TEST",
        gcp_con_id="google_con_different_project",
        bigquery_dest_uri="dv-familie-dev-f48b.oracle_bq_test_apen_data.fra_oracle_fp",
    )

    oracle_to_bq
import datetime, os, json
from os import getenv
from google.cloud import secretmanager

import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from datetime import datetime

def get_periode():
    """
    henter periode for the tidligere måneden eksample--> i dag er 19.04.2022, metoden vil kalkulerer periode aarMaaned eks) '202203'
    :param periode:
    :return: periode
    """
    today = datetime.date.today() # dato for idag 2022-04-19
    first = today.replace(day=1) # dato for første dag i måneden 2022-04-01
    lastMonth = first - datetime.timedelta(days=1) # dato for siste dag i tidligere måneden

    return lastMonth.strftime("%Y%m") # henter bare aar og maaned


def set_secrets_as_envs():
    secrets = secretmanager.SecretManagerServiceClient()
    resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
    secret = secrets.access_secret_version(name=resource_name)
    secret_str = secret.payload.data.decode('UTF-8')
    secrets = json.loads(secret_str)
    os.environ.update(secrets)

  
def oracle_secrets():
    set_secrets_as_envs()
    return dict(
    user=getenv("DBT_ORCL_USER"),
    password=getenv("DBT_ORCL_PASS"),
    host = getenv("DBT_ORCL_HOST"),
    service = getenv("DBT_ORCL_SERVICE"),
    encoding="UTF-8",
    nencoding="UTF-8"
    )

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
        bucket="oracle_bq_test_apen_data",
        filename=oracle_table,
        export_format="csv"
    )

    bucket_to_bq = GCSToBigQueryOperator(
        task_id="bucket-to-bq",
        bucket="oracle_bq_test_apen_data",
        gcp_conn_id=gcp_con_id,
        destination_project_dataset_table=bigquery_dest_uri,
        impersonation_chain=f"{os.getenv('TEAM')}@knada-gcp.iam.gserviceaccount.com",
        autodetect=True,
        write_disposition=write_disposition,
        source_objects=oracle_table,
        source_format="csv"
    )

    delete_from_bucket = GoogleCloudStorageDeleteOperator(
        task_id="delete-from-bucket",
        bucket_name="oracle_bq_test_apen_data",
        objects=[oracle_table],
        gcp_conn_id=gcp_con_id,
    )

    return oracle_to_bucket >> bucket_to_bq >> delete_from_bucket




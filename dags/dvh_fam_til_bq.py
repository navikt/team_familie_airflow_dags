import os
from airflow import DAG
from airflow.models import Variable
import kubernetes.client as k8s
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from datetime import datetime

settings = Variable.get("oracle_table", deserialize_json=True)

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
        task_id=f"{oracle_table}-oracle-to-bucket",
        oracle_conn_id=oracle_con_id,
        gcp_conn_id=gcp_con_id,
        impersonation_chain=f"{os.getenv('TEAM')}@knada-gcp.iam.gserviceaccount.com",
        sql=sql,
        bucket="dvh_fam",
        filename=oracle_table,
        export_format="csv",
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "dmv09-scan.adeo.no:1521"})
            )
        }
    )

    bucket_to_bq = GCSToBigQueryOperator(
        task_id=f"{oracle_table}-bucket-to-bq",
        bucket="dvh_fam",
        gcp_conn_id=gcp_con_id,
        destination_project_dataset_table=bigquery_dest_uri,
        impersonation_chain=f"{os.getenv('TEAM')}@knada-gcp.iam.gserviceaccount.com",
        autodetect=True,
        write_disposition=write_disposition,
        source_objects=oracle_table,
        source_format="csv"
    )

    delete_from_bucket = GoogleCloudStorageDeleteOperator(
        task_id=f"{oracle_table}-delete-from-bucket",
        bucket_name="dvh_fam",
        objects=[oracle_table],
        gcp_conn_id=gcp_con_id,
    )

    return oracle_to_bucket >> bucket_to_bq >> delete_from_bucket


with DAG('DVH_FAM_Til_BigQuery', start_date=datetime(2023, 11, 29), schedule='0 0 3 * *') as dag:
    for v in settings:
        tabellnavn = v["tabellnavn"]
        schema = v["schema"]
        agg_fam = oracle_to_bigquery(
            oracle_con_id="oracle_con",
            oracle_table= f"{schema}.{tabellnavn}", # oracle_table hentes fra airflow->admin->variables. Det går sjappere å endre tabellnavn der enn å gjøre det i selv dagen!    "agg_fam_bt_eos_kpi", 
            gcp_con_id="google_con_different_project",
            bigquery_dest_uri=f"dv-familie-prod-17e7.dvh_fam.{tabellnavn}",
        )
        agg_fam
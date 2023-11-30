import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator


def oracle_to_bigquery(
    oracle_con_id: str,
    oracle_table: str,
    gcp_con_id: str,
    bigquery_dest_uri: str,
    task_name: str,
    columns: list = [],
):
    columns = ",".join(columns) if len(columns) > 0 else "*"
    write_disposition = "WRITE_TRUNCATE"
    sql=f"SELECT {columns} FROM {oracle_table}"

    globals()[f"oracle_to_bucket+{task_name}"] = OracleToGCSOperator(
        task_id="oracle-to-bucket",
        oracle_conn_id=oracle_con_id,
        gcp_conn_id=gcp_con_id,
        impersonation_chain=f"{os.getenv('TEAM')}@knada-gcp.iam.gserviceaccount.com",
        sql=sql,
        bucket="oracle_bq_test_apen_data",
        filename=oracle_table,
        export_format="csv"
    )

    globals()[f"bucket_to_bq+{task_name}"] = GCSToBigQueryOperator(
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

    globals()[f"delete_from_bucket+{task_name}"] = GoogleCloudStorageDeleteOperator(
        task_id="delete-from-bucket",
        bucket_name="oracle_bq_test_apen_data",
        objects=[oracle_table],
        gcp_conn_id=gcp_con_id,
    )

    return globals()[f"oracle_to_bucket+{task_name}"]  >> globals()[f"bucket_to_bq+{task_name}"] >> globals()[f"delete_from_bucket+{task_name}"]


with DAG(
    dag_id="oracle_til_BQ_ny_test",
    start_date=datetime(2023, 11, 30),
    schedule=None
) as dag:

    oracle_to_bq = PythonOperator(
        task_id = 'task1',
        python_callable = oracle_to_bigquery,
        op_kwargs = {"oracle_con_id":"oracle_con",
            "oracle_table":"FAM_ORACLE_BIGQUERY_TEST",
            "gcp_con_id":"google_con_different_project",
            "bigquery_dest_uri":"dv-familie-dev-f48b.test.fra_oracle_fp",
            "task_name":"{{ task_instance_key_str }}",
            }
    )
    
    oracle_to_bq_test = PythonOperator(
        task_id = 'task2',
        python_callable = oracle_to_bigquery,
        op_kwargs = {"oracle_con_id":"oracle_con",
            "oracle_table":"FAM_ORACLE_BIGQUERY_TEST",
            "gcp_con_id":"google_con_different_project",
            "bigquery_dest_uri":"dv-familie-dev-f48b.test.fra_oracle_fp",
            "task_name":"{{ task_instance_key_str }}",
            }
    )

oracle_to_bq >> oracle_to_bq_test







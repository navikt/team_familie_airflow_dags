from airflow.models import DAG
from airflow.utils.dates import datetime, timedelta
from airflow.operators.python import PythonOperator
from utils.db import oracle_conn
from Oracle_python import fam_ef_patch_ybarn
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task


default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

conn, cur = oracle_conn.oracle_conn()
periode = fam_ef_patch_ybarn.get_periode() 

op_kwargs = {
    'conn': conn,
    'cur': cur
}


with DAG(
    dag_id = 'Fam_EF_patching_ybarn_arena', 
    description = 'An Airflow DAG that invokes "FAM_EF.fam_ef_patch_infotrygd_arena" stored procedure',
    default_args = default_args,
    start_date = datetime(2022, 10, 1), # start date for the dag
    schedule_interval = '0 0 5 * *' , # 5te hver måned,
    catchup = False # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running
) as dag:

    @task
    def notification_start():
        slack_info(
            message = "Patching av ybarn til fam_ef_stonad tabellen starter nå ved hjelp av Airflow! :rocket:"
        )

    start_alert = notification_start()

    send_context_information =  PythonOperator(
        task_id='send_context', 
        python_callable=fam_ef_patch_ybarn.send_context,
        op_kwargs = op_kwargs
        )

    patch_ybarn_arena =  PythonOperator(
        task_id='fam_ef_patch_ybarn_infotrygd_arena', 
        python_callable=fam_ef_patch_ybarn.patch_ybarn_arena,
        op_kwargs = {**op_kwargs, 'periode':periode}
        )

    @task
    def notification_end():
        slack_info(
            message = "Fam_Ef_patch_ybarn_infotrygd_arena er nå ferdig kjørt! :tada: :tada:"
        )
    slutt_alert = notification_end()

start_alert >> send_context_information >> patch_ybarn_arena >> slutt_alert

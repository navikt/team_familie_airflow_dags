from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
import os
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
from dataverk_airflow.knada_operators import create_knada_python_pod_operator
from felles_metoder import set_secrets_as_envs, get_periode, send_context
from utils.db.oracle_conn import oracle_conn, oracle_conn_close


default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

with DAG(
    dag_id = 'Fam_EF_patching_ybarn_arena', 
    description = 'An Airflow DAG that invokes "FAM_EF.fam_ef_patch_infotrygd_arena" stored procedure',
    default_args = default_args,
    start_date = datetime(2022, 10, 1), # start date for the dag
    schedule_interval = None,#'0 0 5 * *' , # 5te hver måned,
    catchup = False # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running
) as dag:


    @task
    def notification_start():
        slack_info(
            message = "Patching av ybarn til fam_ef_stonad tabellen starter nå ved hjelp av Airflow! :rocket:"
        )

    start_alert = notification_start()

    @task
    def patch_ybarn():
        set_secrets_as_envs()
        periode = get_periode()
        conn, cur = oracle_conn()
        action_name = 'Patcher ybarn til fam_ef_stonad'
        send_context(conn, cur, action_name)
        sql = (f"""
            DECLARE
                P_IN_PERIODE_YYYYMM NUMBER;
                P_OUT_ERROR VARCHAR2(200);
    
            BEGIN
            P_IN_PERIODE_YYYYMM := {periode};
            P_OUT_ERROR := NULL;
            FAM_EF.fam_ef_patch_infotrygd_arena (  P_IN_PERIODE_YYYYMM => P_IN_PERIODE_YYYYMM,P_OUT_ERROR => P_OUT_ERROR) ;  
            END;
        """)
        cur.execute(sql)
        conn.commit()
        oracle_conn_close()
 
    patch_ybarn_arena = patch_ybarn()
 

    @task
    def notification_end():
        slack_info(
            message = "Fam_Ef_patch_ybarn_infotrygd_arena er nå ferdig kjørt! :tada: :tada:"
        )
    slutt_alert = notification_end()

start_alert >> patch_ybarn_arena >> slutt_alert

#start_alert >> send_context_information >> patch_ybarn_arena >> slutt_alert

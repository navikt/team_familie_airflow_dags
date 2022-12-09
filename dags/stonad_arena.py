from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from dataverk_airflow.knada_operators import create_knada_python_pod_operator
from airflow.operators.python import PythonOperator
from utils.db import oracle_conn
from Oracle_python import fam_ef_stonad_arena_methods
from Oracle_python import fam_ef_vedtak_arena_methods
from operators.slack_operator import slack_info, slack_error
from airflow.decorators import task
import os

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

conn, cur = oracle_conn.oracle_conn()
periode = fam_ef_stonad_arena_methods.get_periode() 

op_kwargs = {
    'conn': conn,
    'cur': cur
}


with DAG(
    dag_id = 'Sas_erstatning', 
    description = 'An Airflow DAG to invoke dbt stonad_arena project and a Python script to insert into fam_ef_stonad_arena ',
    default_args = default_args,
    start_date = datetime(2022, 8, 1), # start date for the dag
    schedule_interval = '@monthly' , #timedelta(days=1), schedule_interval='*/5 * * * *',
    catchup = False # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running
) as dag:

    @task
    def notification_start():
        slack_info(
            message = "Lasting av data til både fam_ef_arena_stonad og fam_ef_arena_vedtak starter nå ved hjelp av Airflow! :rocket:"
        )

    start_alert = notification_start()

    dbt_run = create_knada_python_pod_operator(
        dag = dag, 
        name = 'dbt-run',
        repo = 'navikt/dvh_familie_dbt',
        script_path = 'airflow/dbt_run.py',
        #namespace = os.getenv('NAMESPACE'),
        branch = 'main',
        do_xcom_push = True, 
        extra_envs={
            'DBT_COMMAND': """run --vars '{{"periode":{}}}'""".format(periode), #"""run --vars '{{"periode":"{}}}'""".format(periode), #'run --vars {}'.format(periode), # 'samme som i dbt terminalen men uten dbt. ex) dbt run -model blabla'
            'LOG_LEVEL': 'DEBUG',
            'DB_SCHEMA': 'dvh_fam_ef',
            'KNADA_TEAM_SECRET': os.getenv('KNADA_TEAM_SECRET')
        },
        slack_channel='#dv-team-familie-varslinger'   
    )

    send_context_information =  PythonOperator(
        task_id='send_context', 
        python_callable=fam_ef_stonad_arena_methods.send_context,
        op_kwargs = op_kwargs
        )

    delete_periode_fra_fam_ef_stonad_arena =  PythonOperator(
        task_id='delete_periode_stonad_arena', 
        python_callable=fam_ef_stonad_arena_methods.delete_data,

        op_kwargs = {**op_kwargs, 'periode':periode}
        )

    insert_periode_into_fam_ef_stonad_arena =  PythonOperator(
        task_id='insert_periode_stonad_arena', 
        python_callable=fam_ef_stonad_arena_methods.insert_data,
        op_kwargs = op_kwargs
        )

    delete_periode_fra_fam_ef_vedtak_arena =  PythonOperator(
        task_id='delete_periode_vedtak_arena', 
        python_callable=fam_ef_vedtak_arena_methods.delete_data,
        op_kwargs = {**op_kwargs, 'periode':periode}
        )

    insert_periode_into_fam_ef_vedtak_arena =  PythonOperator(
        task_id='insert_periode_vedtak_arena', 
        python_callable=fam_ef_vedtak_arena_methods.insert_data,
        op_kwargs = op_kwargs
        )

    close_db_conn = PythonOperator(
        task_id='close_db_conn', 
        python_callable = oracle_conn.oracle_conn_close,
        op_kwargs = {'conn': conn}
        )
    
    @task
    def notification_end():
        slack_info(
            message = "Data er feridg lastet til fam_ef_arena_stonad og fam_ef_arena_vedtak! :tada: :tada:"
        )
    slutt_alert = notification_end()


    
start_alert >> dbt_run >> delete_periode_fra_fam_ef_stonad_arena >> insert_periode_into_fam_ef_stonad_arena 
start_alert >> dbt_run >> delete_periode_fra_fam_ef_vedtak_arena >> insert_periode_into_fam_ef_vedtak_arena >> close_db_conn >> slutt_alert
start_alert >> dbt_run >> send_context_information

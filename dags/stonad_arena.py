#from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from dataverk_airflow.knada_operators import create_knada_python_pod_operator
from airflow.operators.python import PythonOperator
from utils.db import oracle_conn
from Oracle_python import fam_ef_stonad_arena_methods

default_args = {'owner': 'Team-Familie', 'retries': 3, 'retry_delay': timedelta(minutes=1)}
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
    start_date = datetime(2022, 8, 28), # start date for the dag
    schedule_interval = '@monthly' , #timedelta(days=1), schedule_interval='*/5 * * * *',
    catchup = False # makes only the latest non-triggered dag runs by airflow (avoid having all dags between start_date and current date running)
) as dag:

    dbt_run = create_knada_python_pod_operator(
        dag = dag,
        name = 'dbt-run',
        repo = 'navikt/dvh_familie_dbt',
        script_path = 'airflow/dbt_run.py',
        namespace = Variable.get("NAMESPACE"),
        branch = 'main',
        do_xcom_push = True,
        extra_envs={
            'DBT_COMMAND': 'run --vars "{periode: 202207}"', # 'samme som i dbt terminalen men uten dbt. ex) dbt run -model blabla'
            'LOG_LEVEL': 'DEBUG',
            'DB_SCHEMA': 'dvh_fam_ef'
        },
        slack_channel='#dv-team-familie-varslinger'
    )

    send_context_information =  PythonOperator(
        task_id='send_context', 
        python_callable=fam_ef_stonad_arena_methods.send_context,
        op_kwargs = op_kwargs)

    delete_periode_fra_fam_e_stonad_arena =  PythonOperator(
        task_id='delete_periode', 
        python_callable=fam_ef_stonad_arena_methods.delete_data,
        op_kwargs = op_kwargs)

    insert_periode_into_fam_e_stonad_arena =  PythonOperator(
        task_id='insert_periode', 
        python_callable=fam_ef_stonad_arena_methods.insert_data,
        op_kwargs = op_kwargs)

    close_db_conn = PythonOperator(
        task_id='close_db_conn', 
        python_callable = oracle_conn.oracle_conn_close,
        op_kwargs = {'conn': conn})

    # insert_into_stonad_arena = create_knada_python_pod_operator(
    #     dag = dag,
    #     name = "insert_into_stonad_arena",
    #     repo = 'navikt/dvh_familie_dbt',
    #     script_path = "airflow/insert_into_ef_stonad_arena.py",
    #     namespace = Variable.get("NAMESPACE"),
    #     branch = 'main',
    #     #do_xcom_push = True,
    #     slack_channel='#dv-team-familie-varslinger'
    # )
    
dbt_run >> send_context_information
dbt_run >> delete_periode_fra_fam_e_stonad_arena >> insert_periode_into_fam_e_stonad_arena >> close_db_conn

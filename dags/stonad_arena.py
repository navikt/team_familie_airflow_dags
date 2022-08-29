from datetime import datetime, timedelta
from airflow.models import DAG, Variable
#from airflow.utils.dates import datetime
from dataverk_airflow.knada_operators import create_knada_python_pod_operator

default_args = {'owner': 'Team-Familie', 'retries': 3, 'retry_delay': timedelta(minutes=1)}

with DAG(
    dag_id = 'dbt_dag', 
    description = 'An Airflow DAG to invoke dbt stonad_arena project and a Python script to insert into fam_ef_stonad_arena ',
    default_args = default_args,
    start_date = datetime(2022, 8, 1), # start date for the dag
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
            'DBT_COMMAND': 'run', # 'samme som i dbt terminalen men uten dbt. ex) dbt run -model blabla'
            'LOG_LEVEL': 'DEBUG',
            'DB_SCHEMA': 'dvh_fam_ef'
        },
        slack_channel='#dv-team-familie-varslinger'
    )

    insert_into_stonad_arena = create_knada_python_pod_operator(
        dag = dag,
        name = "insert_into_stonad_arena",
        repo = 'navikt/dvh_familie_dbt',
        script_path = "airflow/insert_into_ef_stonad_arena.py",
        namespace = Variable.get("NAMESPACE"),
        branch = 'main',
        #do_xcom_push = True,
        slack_channel='#dv-team-familie-varslinger'
    )
    
dbt_run >> insert_into_stonad_arena
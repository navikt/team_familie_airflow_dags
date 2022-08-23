from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow.knada_operators import create_knada_python_pod_operator

with DAG(
    dag_id = 'dbt_dag', 
    start_date = datetime(2022, 9, 1), # start date for the dag
    description = 'An Airflow DAG to invoke dbt stonad_arena project and a Python script to insert into fam_ef_stonad_arena ',
    schedule_interval = None, #'@monthly' , #timedelta(days=1), schedule_interval='*/5 * * * *',
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
            'DBT_COMMAND': 'run', # 'samme som i dbt terminal med uten dbt ex) dbt run -model blabla'
            'LOG_LEVEL': 'INFO',
            'DB_SCHEMA': 'dvh_fam_ef'
        },
        slack_channel='#dv-team-familie-varslinger'
    )

    insert_to_fam_ef_stonad_arena = create_knada_python_pod_operator(
        dag = dag,
        name = "insert_to_fam_ef_stonad_arena",
        repo = 'navikt/dvh_familie_dbt',
        script_path = "airflow/insert_into_script.py",
        namespace = Variable.get("NAMESPACE"),
        branch = 'main',
        do_xcom_push = True,
        slack_channel='#dv-team-familie-varslinger'
    )
    
dbt_run >> insert_to_fam_ef_stonad_arena



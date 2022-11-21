from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime, timedelta
from airflow.models import Variable
from operators.slack_operator import slack_info, slack_error
from dataverk_airflow.knada_operators import create_knada_python_pod_operator


from operators.dbt_operator import create_dbt_operator

default_args = {
    #'owner': 'Team-Familie',
    #'retries': 1,
    #'on_failure_callback': slack_error
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ef_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]


with DAG('test_data_lasting', 
        default_args=default_args,
        schedule_interval = '*/10 * * * *', #'0 10 * * *', #hver dag kl 06:00 om morgenene   
        start_date = datetime(2022, 11, 21),
        catchup = False
        ) as dag:

    t_start = DummyOperator(task_id='start_task', dag=dag)
    t_stop_opp = DummyOperator(task_id='stop_task', dag=dag)

    t_run_dbt = create_knada_python_pod_operator(
        dag=dag,
        name="unpack_all_new_kafka_løsning",
        branch = 'main',
        #dbt_command="run --select test.ef_ny_consument_test",
        #db_schema=v_schema
        repo = 'navikt/dvh_familie_dbt',
        script_path = 'airflow/dbt_run.py',
        namespace = Variable.get("NAMESPACE"),
        do_xcom_push = True, 
        extra_envs={
            'DBT_COMMAND': "run --select test.ef_ny_consument_test",
            'DB_SCHEMA': 'dvh_fam_ef'
        }
    )



t_start >>  t_run_dbt >>  t_stop_opp
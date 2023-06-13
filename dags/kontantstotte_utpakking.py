from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from operators.slack_operator import slack_info, slack_error

from operators.dbt_operator import create_dbt_operator

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_ks_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]


with DAG(
        dag_id ='kontantstotte_meldinger_utpakking', 
        default_args=default_args,
        start_date = datetime(2023, 3, 27),
        schedule_interval = '15 * * * *',  
        catchup = False
        ) as dag:

    t_start = DummyOperator(task_id='start_task', dag=dag)

    t_stop_opp = DummyOperator(task_id='stop_task', dag=dag)

    ks_utpakking_dbt = create_dbt_operator(
        dag=dag,
        name="utpakking_ks",
        script_path = 'airflow/dbt_run_test.py',
        branch=v_branch,
        dbt_command="run --select KS_transformasjon.*",
        db_schema=v_schema
    )

t_start >>  ks_utpakking_dbt >>  t_stop_opp
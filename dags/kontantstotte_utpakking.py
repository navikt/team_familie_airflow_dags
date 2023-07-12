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
        schedule_interval = '20 * * * *',  # at minute 20 every hour
        catchup = False
        ) as dag:

    t_start = DummyOperator(task_id='start_task', dag=dag)

    t_stop_opp = DummyOperator(task_id='stop_task', dag=dag)

    ks_utpakking_dbt = create_dbt_operator(
        dag=dag,
        name="utpakking_ks",
        script_path = 'airflow/dbt_run_test.py',
        branch=v_branch,
        dbt_command= """run --select KS_transformasjon.* --vars "{{dag_interval_start: '{0}', dag_interval_end: '{1}'}}" """.format('{{ execution_date.in_timezone("Europe/Amsterdam").strftime("%Y-%m-%d %H:%M:%S")}}','{{ (execution_date + macros.timedelta(hours=1)).in_timezone("Europe/Amsterdam").strftime("%Y-%m-%d %H:%M:%S")}}'),
        db_schema=v_schema
    )

t_start >>  ks_utpakking_dbt >>  t_stop_opp
from airflow.models import DagRun
from airflow.models import DAG
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
  dag_id='Helen_tester',
  default_args = default_args,
  schedule_interval= None,
  max_active_runs=1,
  catchup = False
) as dag:
    
    def get_most_recent_dag_run():
        dag_id = 'FP_konsument'
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return dag_runs[0] if dag_runs else None
    
    def get():
        dag_run = get_most_recent_dag_run
        if dag_run:
            print(f'The most recent DagRun was executed at: ')

    dag_run = PythonOperator(
        task_id='Helen_tester',
        python_callable=get,
        dag=dag
    )
dag_run
from airflow.models import DagRun
from airflow.operators.python_operator import PythonOperator

with DAG(
  dag_id="Helen tester",
  start_date=datetime(2024, 5, 30),
  default_args = default_args,
  schedule_interval= None,
  max_active_runs=1,
  catchup = False
) as dag:
    
    def get_most_recent_dag_run(dag_id):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return dag_runs[0] if dag_runs else None

    dag_run = PythonOperator(
        task_id='Helen tester',
        python_callable=get_most_recent_dag_run('FP_konsument'),
        dag=dag
    )
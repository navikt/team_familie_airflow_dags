from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow.knada_operators import create_knada_python_pod_operator
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id = 'test_dag', 
    start_date = datetime(2022, 9, 1), # start date for the dag
    schedule_interval = None, #'@monthly' , #timedelta(days=1), schedule_interval='*/5 * * * *',
) as dag:

    test_task = DummyOperator(
        task_id = 'test'
    )

    
test_task

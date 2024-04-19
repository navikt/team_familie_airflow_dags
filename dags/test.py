from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow import python_operator
from airflow.operators.dummy_operator import DummyOperator
from operators.slack_operator import slack_info
from airflow.decorators import task


with DAG(
    dag_id = 'test_dag', 
    start_date = datetime(2023, 4, 1), # start date for the dag
    schedule_interval = None, #'@monthly' , # schedule_interval='*/5 * * * *',
) as dag:

    @task
    def slack():
        slack_info(
            message = "en test"
        )

 
    test = slack()
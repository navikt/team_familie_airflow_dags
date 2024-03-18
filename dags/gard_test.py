from airflow.models import DAG
from airflow.utils.dates import datetime
from operators.slack_operator import slack_info
from airflow.decorators import task

with DAG(
    dag_id = "gard_test",
    start_date = datetime(2024, 3, 18),
    schedule_interval = None,
) as dag: 
    @task
    def slack():
        slack_info(
            message = "en GARD test"
        )

    gard = slack()
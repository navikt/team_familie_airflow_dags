from airflow.models import DAG, Variable
from airflow.utils.dates import datetime
from dataverk_airflow.knada_operators import create_knada_nb_pod_operator
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_info

with DAG(
  dag_id = 'kopier_BS_data_fra_BQ_til_Oracle',
  description = 'kopierer brillestonad data fra en tabell i BigQuery til en tabell i Oracle database',
  start_date=datetime(2023, 2, 21),
  schedule_interval= '@daily',
  max_active_runs=1,
  catchup = False
) as dag:

    @task
    def notification_start():
        slack_info(
            message = "Kopiering  av brillestønads data fra BigQuery til Oracle starter nå! :rocket:"
        )

    start_alert = notification_start()

    bs_data_kopiering = create_knada_nb_pod_operator(
    dag = dag,
    name = 'BS_data_kopiering',
    repo = 'navikt/dvh-fam-notebooks',
    nb_path = 'HM/kopier_BS_data_til_oracle.ipynb',
    branch = 'main',
    #delete_on_finish= False,
    resources=client.V1ResourceRequirements(
        requests={'memory': '4G'},
        limits={'memory': '4G'}),
    slack_channel = Variable.get('slack_error_channel'),
    log_output=False
    )

    @task
    def notification_end():
        slack_info(
            message = "Kopiering av brillestønads data fra BigQuery til Oracle er vellykket! :tada: :tada:"
        )
    slutt_alert = notification_end()

start_alert >> bs_data_kopiering >> slutt_alert



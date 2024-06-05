from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from kubernetes import client
from airflow.operators.python_operator import PythonOperator
from airflow.models import DagRun
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from airflow import settings
from operators.slack_operator import slack_error, slack_info
#from utils.db.oracle_conn import oracle_conn
from allowlists.allowlist import prod_oracle_slack, dev_oracle_slack, r_oracle_slack

miljo = Variable.get('miljo')   
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_slack)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_slack)   									  
else:
    allowlist.extend(dev_oracle_slack)
    miljo = 'dev' # Har her ingen verdi, så ønsker å sette verdi for å bruke direkte i string i rapport

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': datetime.datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'retry_delay': datetime(minute=5),
    'sla': timedelta(seconds=1), #Test av SLA
    'email': ['gard.sigurd.troim.henriksen@nav.no'],
    'on_failure_callback': slack_error,

}
# Define the DAG
with DAG(
    dag_id = 'suksessrapport',
    default_args=default_args,
    description='Count the number of successful DAG runs for each DAG',
    start_date=datetime(2024, 6, 5),
    schedule_interval= "1 1 * * *", # kl 13:00 CEST hver dag
    catchup=False
) as dag:

    @task(
            executor_config={
                "pod_override": client.V1Pod(
                    metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
                )
            }
        )  

    def count_successful_dag_runs():
        # Set up the session
        Session = sessionmaker()
        engine = create_engine(settings.SQL_ALCHEMY_CONN)
        Session.configure(bind=engine)
        session = Session()
        yesterday = datetime.datetime.now(datetime.timezone.utc).replace(hour = 0, minute = 0, second = 0, microsecond = 0) - datetime.timedelta(days=1) - datetime.timedelta(hours=2)
        today = datetime.datetime.now(datetime.timezone.utc).replace(hour = 0, minute = 0, second = 0, microsecond = 0) - datetime.timedelta(hours=2)
        string_of_successful_runs = ""

        try:
            #today = datetime.now(datetime.CEST)
            #yesterday = datetime.now(datetime.CEST) - datetime.timedelta(days=1)
            #last_day = current_time -  datetime.timedelta(days=1)
            # Query for the count of successful DAG runs
            success_counts = session.query(
                DagRun.dag_id, func.count(DagRun.dag_id).label('success_count')
            ).filter(DagRun.state == 'success',  DagRun.execution_date >= yesterday, DagRun.execution_date < today).group_by(DagRun.dag_id).all()

            # Process the results (print to log, store in another table, etc.)
            for dag_id, success_count in success_counts:
                #print(f"DAG ID: {dag_id}, Success Count: {success_count}")
                # You can also store this result in another table if needed
                #string_of_successful_runs += f"DAG ID: {dag_id}, Success Count: {success_count}\n"
                string_of_successful_runs = "\n ".join(f"DAG ID: {dag_id}, Success Count: {success_count}")
                

        except Exception as e:
            #print(f"Error counting successful DAG runs: {e}")
            slack_info(
                message=f"Error counting successful DAG runs: {e}",
            )

        finally:
            session.close()

            report_summary = f"""
*Antall suksesfulle {miljo} DAG runs, mellom {yesterday} og {today}:*
```
{string_of_successful_runs}
```
"""
            #Post result to slack
            slack_info(
            message=f"{report_summary}",
            )

    
    # # Define the task
    # count_task = PythonOperator(
    #     task_id='count_successful_dag_runs_task',
    #     python_callable=count_successful_dag_runs,
    #     dag=dag,
    # )

post_til_info_slack = count_successful_dag_runs()

# Set task dependencies
post_til_info_slack

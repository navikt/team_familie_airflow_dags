from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import DagRun
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from airflow import settings
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'count_successful_dag_runs',
    default_args=default_args,
    description='Count the number of successful DAG runs for each DAG',
    schedule_interval=timedelta(days=1),
)

def count_successful_dag_runs():
    # Set up the session
    Session = sessionmaker()
    engine = create_engine(settings.SQL_ALCHEMY_CONN)
    Session.configure(bind=engine)
    session = Session()

    try:
        current_time = datetime.today()
        #last_day = current_time -  datetime.timedelta(days=1)
        # Query for the count of successful DAG runs
        success_counts = session.query(
            DagRun.dag_id, func.count(DagRun.dag_id).label('success_count')
        ).filter(DagRun.state == 'success',  DagRun.time == current_time).group_by(DagRun.dag_id).all()

        # Process the results (print to log, store in another table, etc.)
        for dag_id, success_count in success_counts:
            print(f"DAG ID: {dag_id}, Success Count: {success_count}")
            # You can also store this result in another table if needed

    except Exception as e:
        print(f"Error counting successful DAG runs: {e}")

    finally:
        session.close()

# Define the task
count_task = PythonOperator(
    task_id='count_successful_dag_runs_task',
    python_callable=count_successful_dag_runs,
    dag=dag,
)

# Set task dependencies
count_task

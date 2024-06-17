from datetime import datetime
from datetime import date
from datetime import timedelta
import datetime as dt
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from kubernetes import client
from airflow.models import DagRun
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from airflow import settings
from operators.slack_operator import slack_error, slack_info
from allowlists.allowlist import prod_oracle_slack, dev_oracle_slack, r_oracle_slack

miljo = Variable.get('miljo')   
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_slack)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_slack)   									  
else:
    allowlist.extend(dev_oracle_slack)
    miljo = 'dev'

# Modifisere default args for DAG-en
default_args = {
    #'sla': timedelta(seconds=1), # Test av SLA flyttet fra DAG til en task
    'email': ['gard.sigurd.troim.henriksen@nav.no'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': slack_error,
    'retries': 0,
}

with DAG(
    dag_id = 'Suksessrapport',
    default_args=default_args,
    description='Count the number of successful DAG runs for each DAG',
    start_date=datetime(2024, 6, 5),
    schedule_interval= "0 11 * * *", # Kjører kl 13:00 CEST hver dag
    catchup=False
    
) as dag:

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
        },
        task_id='count_successful_dag_runs_task',
        dag=dag,
    )  
    def count_successful_dag_runs():
        # Sette opp session
        Session = sessionmaker()
        engine = create_engine(settings.SQL_ALCHEMY_CONN)
        Session.configure(bind=engine)
        session = Session()
        string_of_successful_runs = ""
        # 10:00:00+00:00 til 10:00:00+00:00 UTC (12:00 CEST, DAG kjører 13:00 CEST)
        yesterday = dt.datetime.now(dt.timezone.utc).replace(hour = 10, minute = 0, second = 0, microsecond = 0) - dt.timedelta(days=1)
        today = dt.datetime.now(dt.timezone.utc).replace(hour = 10, minute = 0, second = 0, microsecond = 0)
        print(str(today) + " & " + str(yesterday))

        try:
            # Query for tellingen av suksessfulle DAG-kjøringer
            # TODO: Kan i fremtiden også sjekke for failed kjøringer, eller andre 'states'
            success_counts = session.query(
                DagRun.dag_id, func.count(DagRun.dag_id).label('success_count')
            ).filter(DagRun.state == 'success',  DagRun.execution_date >= yesterday, DagRun.execution_date < today).group_by(DagRun.dag_id).all()

            # Konkatinerer resultatene i en string
            # TODO: Kan være intressant å gjøre noe her i fremtiden, feks. å sjekke eller gjøre noe med resultat vi får
            for dag_id, success_count in success_counts:
                # Print for å sjekke direkte i loggen i Airflow
                print(f"DAG ID: {dag_id}, Success Count: {success_count}")
                string_of_successful_runs += f"DAG ID: {dag_id}, Success Count: {success_count}\n"

        except Exception as e:
            slack_info(
                message=f"Error counting successful DAG runs: {e}",
            )

        finally:
            session.close()
            # Fjerner to siste tegn, "\n", fra siste konkatinering. NB! "\n" fjernes ved bare -1, ikke -2! Mer vil inkorrekt spise av stringen.
            return string_of_successful_runs[:-1]

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
        },
        task_id='info_slack_task',
        dag=dag,
        sla=timedelta(seconds=0), # Test av SLA, task har tidligere vart så kort at denne ikke kjørte. Håper 0 sekunder vil hjelpe. 
    ) 
    def info_slack(string_of_successful_runs):
        # Henter dato, ikke klokkeslett
        today = date.today()
        yesterday = date.today() - timedelta(days = 1)
        # Hardkodet noe av string
        report_summary = f"""
*Antall suksesfulle {miljo} DAG runs, som har kjørt mellom {yesterday} og {today} kl 12:00 CEST:*
```
{string_of_successful_runs}
```
"""
        # Poster resultat til slack
        slack_info(
        message=f"{report_summary}",
        )


count_task = count_successful_dag_runs()
post_til_info_slack = info_slack(count_task)

count_task >> post_til_info_slack
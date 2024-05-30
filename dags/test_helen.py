from airflow.models import DagRun

def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None


dag_run = get_most_recent_dag_run('FP_konsument')
if dag_run:
    print(f'The most recent DagRun was executed at: {dag_run.execution_date}')
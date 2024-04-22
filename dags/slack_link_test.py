from airflow import DAG
from datetime import datetime
from operators import CustomSlackOperator
from airflow.models import Variable
from airflow.decorators import task
from kubernetes import client
from allowlists.allowlist import prod_oracle_slack, dev_oracle_slack, r_oracle_slack

miljo = Variable.get('miljo')   
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_slack)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_slack)				   									  
else:
    allowlist.extend(dev_oracle_slack)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 19),
}

dag = DAG('slack_link_test', default_args=default_args, schedule_interval=None)

@task(
    executor_config={
        "pod_override": client.V1Pod(
            metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
        )
    }
)
def info_slack():
    CustomSlackOperator(
    task_id='send_slack_message',
    text="Click [here](https://vg.no) to visit our website.",
    dag=dag
)

info_slack()
  
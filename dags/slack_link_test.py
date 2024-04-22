from airflow import DAG
from datetime import datetime
from operators.custom_slack_operator import CustomSlackOperator
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


with DAG(
  dag_id='slack_link_test',
  start_date=datetime(2024, 4, 22),
  schedule_interval= None, # kl 7 hver dag
  catchup=False
) as dag:

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
  
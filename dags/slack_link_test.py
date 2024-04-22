from datetime import datetime
from datetime import date
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_error, slack_info
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from utils.db.oracle_conn import oracle_conn
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
  default_args={'on_failure_callback': slack_error},
  start_date=datetime(2024, 4, 22),
  schedule_interval= None, 
  catchup=False
) as dag:

    @task(
            executor_config={
                "pod_override": client.V1Pod(
                    metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
                )
            }
        )
    def send_slack_message_with_link():
        message_with_link = "Click <here|https://www.vg.no> to visit our website."
        
        slack_operator = SlackAPIPostOperator(
            task_id='send_slack_message',
            channel='#dv-team-familie-varslinger',
            text=message_with_link,
        )
        slack_operator.execute()

    # Example usage:
    send_slack_message_with_link()
  
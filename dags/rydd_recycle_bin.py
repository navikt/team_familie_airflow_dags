from airflow.models import DAG, Variable
from airflow.utils.dates import datetime, timedelta
from kubernetes import client
from operators.slack_operator import slack_error
from airflow.decorators import task
from dataverk_airflow import python_operator
from airflow.operators.python_operator import PythonOperator
from allowlists.allowlist import prod_oracle_conn_id, dev_oracle_conn_id
import oracledb
from felles_metoder.felles_metoder import oracle_secrets
import kubernetes.client as k8s



branch = Variable.get("branch")

miljo = Variable.get('miljo')

allowlist = []
if miljo == 'Prod':
  allowlist.extend(prod_oracle_conn_id)
else:
  allowlist.extend(dev_oracle_conn_id)

default_args = {
    'owner': 'Team-Familie', 
    'retries': 2, 
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_error
    }

def delete_from_recycle_bin():
    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'sletter dbt temp tabeller fra recyle bin' );
        end;
    ''')

    secrets = oracle_secrets()

    for skjema in ["dvh_fam_pp", "dvh_fam_ef", "dvh_fam_bt", "dvh_fam_ks", "dvh_fam_fp"]:
        dsn_tns = oracledb.makedsn(secrets['host'], 1521, service_name = secrets['service'])
        with oracledb.connect(user = secrets['user'][skjema], password = secrets['password'], dsn = dsn_tns) as connection:
            with connection.cursor() as cursor:
                cursor.execute(send_context_sql)
                cursor.execute(f"""BEGIN FOR rec IN (SELECT object_name, original_name FROM  dba_recyclebin WHERE type = 'TABLE' AND OWNER={skjema} AND ORIGINAL_NAME LIKE '%DBT%') LOOP
                            EXECUTE IMMEDIATE 'PURGE TABLE "' || rec.object_name || '"'; END LOOP; END;""")
                connection.commit()

with DAG(
    dag_id = 'rydd_recycle_bin', 
    description = 'An Airflow DAG that deletes dbt_temp tables i recycle bin',
    default_args = default_args,
    start_date = datetime(2024, 6, 3), 
    schedule_interval = '@daily' , 
    catchup = False 
) as dag:
    
    slett_tabeller_recycle_bin  = PythonOperator(
    dag=dag,
    task_id='slett_dbt_tabeller_recycle_bin', 
    wait_for_downstream=False,
    provide_context=True,
    branch=branch,
    python_callable=delete_from_recycle_bin,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources={
                        "requests": {"cpu": "2","memory": "2Gi"}
                        }
                    )
                ]
            ),
            metadata=k8s.V1ObjectMeta(annotations={"allowlist": ",".join(allowlist)})
        )
    },
    requirements_path="requirements.txt",
    )
    
slett_tabeller_recycle_bin
    

from datetime import datetime
import time
from airflow.models import DAG
from airflow.models import Variable
from kosument_config import bt
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error
from allowlists.allowlist import slack_allowlist, prod_oracle_conn_id, dev_oracle_conn_id,r_oracle_conn_id

miljo = Variable.get('miljo')
allowlist = []

if miljo == 'Prod':
    allowlist.extend(prod_oracle_conn_id)
elif miljo == 'test_r':
    allowlist.extend(r_oracle_conn_id)
else:
    allowlist.extend(dev_oracle_conn_id)

default_args = {
    'owner': 'Team-Familie',
    'retries': 1,
    'on_failure_callback': slack_error
}

# Bygger parameter med logging, modeller og miljø
settings = Variable.get("dbt_bt_schema", deserialize_json=True)
v_branch = settings["branch"]
v_schema = settings["schema"]

v_pp_omp_periode = Variable.get("pp_omp_periode")
if not v_pp_omp_periode:
    v_pp_omp_periode = datetime.now().strftime('yyyymm')

v_pp_omp_max_vedtaksperiode = Variable.get("pp_omp_max_vedtaksperiode")
if not v_pp_omp_max_vedtaksperiode:
    v_pp_omp_max_vedtaksperiode = datetime.now().strftime('yyyymm')

with DAG(
  dag_id="PP_omp_manedsprosessering",
  start_date=datetime(2025, 1, 29, 15),
  default_args = default_args,
  schedule_interval= "0 8 5 * *",#"@hourly",
  max_active_runs=1,
  catchup = True
) as dag:

  pp_omp_manedsprosessering_dbt = create_dbt_operator(
      dag=dag,
      name="pp_omp_manedsprosessering",
      repo='navikt/dvh_familie_dbt',
      script_path = 'airflow/dbt_run.py',
      branch=v_branch,
      dbt_command= """run --select PP_manedsprosessering.* --vars '{pp_omp_periode: v_pp_omp_periode, pp_omp_max_vedtaksperiode: v_pp_omp_max_vedtaksperiode}'""",
      db_schema=v_schema,
      allowlist=allowlist
  )

pp_omp_manedsprosessering_dbt


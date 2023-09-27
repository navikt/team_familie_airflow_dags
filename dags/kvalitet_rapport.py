from datetime import datetime
from typing import List
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models import XCom
from airflow.models.taskinstance import TaskInstance
from airflow.utils.timezone import make_aware
from operators.dbt_operator import create_dbt_operator
from operators.slack_operator import slack_error, slack_info
from dataverk_airflow.knada_operators import create_knada_python_pod_operator
from utils.db.oracle_conn import oracle_conn

with DAG(
  dag_id='datakvalitetsrapport',
  default_args={'on_failure_callback': slack_error},
  start_date=datetime(2023, 9, 27),
  schedule_interval= None,#"0 6 * * *", # kl 6 hver dag
  catchup=False
) as dag:

  @task
  def hent_kafka_last():
    bt_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_BT.fam_bt_meta_data WHERE lastet_dato >= sysdate - 1
    """
    ef_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_EF.fam_ef_meta_data WHERE lastet_dato >= sysdate - 1
    """
    ks_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_KS.fam_ks_meta_data WHERE lastet_dato >= sysdate - 1
    """
    sjekk_hull_i_BT_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_partition, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic, kafka_partition
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_BT.fam_bt_meta_data)
    where kafka_offset > 766801 and  neste-kafka_offset > 1
    """
    sjekk_hull_i_EF_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_partition, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic, kafka_partition
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_EF.fam_ef_meta_data)
        where lastet_dato > to_date('01.08.2023', 'dd.mm.yyyy') and  neste-kafka_offset > 1
    """
    sjekk_hull_i_KS_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_partisjon, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic, kafka_partisjon
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_KS.fam_ks_meta_data)
        where neste-kafka_offset > 1 and lastet_dato > to_date('27.09.2023', 'dd.mm.yyyy')
    """
    with oracle_conn().cursor() as cur:
        bt_ant = cur.execute(bt_ant_mottatt_mldinger).fetchone()[0]
        bt_hull = cur.execute(sjekk_hull_i_BT_meta_data).fetchone()
        ef_ant = cur.execute(ef_ant_mottatt_mldinger).fetchone()[0]
        ef_hull = cur.execute(sjekk_hull_i_EF_meta_data).fetchone()
        ks_ant = cur.execute(ks_ant_mottatt_mldinger).fetchone()[0]
        ks_hull = cur.execute(sjekk_hull_i_KS_meta_data).fetchone()
    return [bt_ant,bt_hull,ef_ant,ef_hull,ks_ant,ks_hull]


  @task
  def info_slack(kafka_last):
    [
      bt_ant,bt_hull,
      ef_ant,ef_hull,
      ks_ant,ks_hull,
    ] = kafka_last
    bt_antall_meldinger =        f"Antall mottatt BT meldinger.......{str(bt_ant).rjust(7, '.')}"
    bt_hull_i_meta_data =        f"Sjekk for manglene kafka_offset i BT_meta_data:............{str(bt_hull).rjust(7, '.')}"
    ef_antall_meldinger =        f"Antall mottatt EF meldinger.......{str(ef_ant).rjust(7, '.')}"
    ef_hull_i_meta_data =        f"Sjekk for manglene kafka_offset i EF_meta_data:............{str(ef_hull).rjust(7, '.')}"
    ks_antall_meldinger =        f"Antall mottatt KS meldinger.......{str(ks_ant).rjust(7, '.')}"
    ks_hull_i_meta_data =        f"Sjekk for manglene kafka_offset i KS_meta_data:............{str(ks_hull).rjust(7, '.')}"
    konsumenter_summary = f"""
*Leste meldinger fra konsumenter siste døgn:*
 
```
{bt_antall_meldinger}
{bt_hull_i_meta_data}
{ef_antall_meldinger}
{ef_hull_i_meta_data}
{ks_antall_meldinger}
{ks_hull_i_meta_data}
```
"""
    kafka_summary = f"*Kafka rapport:*\n{konsumenter_summary}"


    slack_info(
      message=f"{kafka_summary}",
      emoji=":newspaper:"
    )



  kafka_last = hent_kafka_last()
  post_til_info_slack = info_slack(kafka_last)

  kafka_last >> post_til_info_slack
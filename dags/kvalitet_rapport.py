from datetime import datetime
from datetime import date
from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import XCom
from airflow.models.taskinstance import TaskInstance
from operators.slack_operator import slack_error, slack_info
from utils.db.oracle_conn import oracle_conn

with DAG(
  dag_id='datakvalitetsrapport',
  default_args={'on_failure_callback': slack_error},
  start_date=datetime(2023, 9, 27),
  schedule_interval= "0 7 * * *", # kl 7 hver dag
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
    pp_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_PP.fam_pp_meta_data WHERE lastet_dato >= sysdate - 1
    """
    sjekk_hull_i_BT_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_BT.fam_bt_meta_data)
    where kafka_offset > 766801 and  neste-kafka_offset > 1
    """
    sjekk_hull_i_EF_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_EF.fam_ef_meta_data)
        where lastet_dato > to_date('01.08.2023', 'dd.mm.yyyy') and  neste-kafka_offset > 1
    """
    sjekk_hull_i_KS_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_KS.fam_ks_meta_data)
        where neste-kafka_offset > 1 and lastet_dato > to_date('27.09.2023', 'dd.mm.yyyy')
    """
    sjekk_hull_i_PP_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_PP.fam_pp_meta_data)
        where neste-kafka_offset > 1 and lastet_dato > to_date('24.10.2023', 'dd.mm.yyyy')
    """
    with oracle_conn().cursor() as cur:
        bt_ant = cur.execute(bt_ant_mottatt_mldinger).fetchone()[0]
        bt_hull = [str(x) for x in (cur.execute(sjekk_hull_i_BT_meta_data).fetchone() or [])]
        ef_ant = cur.execute(ef_ant_mottatt_mldinger).fetchone()[0]
        ef_hull = [str(x) for x in (cur.execute(sjekk_hull_i_EF_meta_data).fetchone() or [])]
        ks_ant = cur.execute(ks_ant_mottatt_mldinger).fetchone()[0]
        ks_hull = [str(x) for x in (cur.execute(sjekk_hull_i_KS_meta_data).fetchone() or [])]
        pp_ant = cur.execute(pp_ant_mottatt_mldinger).fetchone()[0]
        pp_hull = [str(x) for x in (cur.execute(sjekk_hull_i_PP_meta_data).fetchone() or [])]       
    return [bt_ant,bt_hull,ef_ant,ef_hull,ks_ant,ks_hull,pp_ant,pp_hull]


  @task
  def info_slack(kafka_last):
    gaarsdagensdato = date.today() - timedelta(days = 1)
    [
      bt_ant,bt_hull,
      ef_ant,ef_hull,
      ks_ant,ks_hull,
      pp_ant,pp_hull,     
    ] = kafka_last
    bt_antall_meldinger = f"Antall mottatt BT meldinger for {gaarsdagensdato}......................{str(bt_ant)}"
    bt_hull_i_meta_data = f"Manglene kafka_offset i BT_meta_data for {gaarsdagensdato}:............{str(bt_hull)}"
    ef_antall_meldinger = f"Antall mottatt EF meldinger for {gaarsdagensdato}......................{str(ef_ant)}"
    ef_hull_i_meta_data = f"Manglene kafka_offset i BT_meta_data for {gaarsdagensdato}:............{str(ef_hull)}"
    ks_antall_meldinger = f"Antall mottatt KS meldinger for {gaarsdagensdato}......................{str(ks_ant)}"
    ks_hull_i_meta_data = f"Manglene kafka_offset i BT_meta_data for {gaarsdagensdato}:............{str(ks_hull)}"
    pp_antall_meldinger = f"Antall mottatt PP meldinger for {gaarsdagensdato}......................{str(pp_ant)}"
    pp_hull_i_meta_data = f"Manglene kafka_offset i PP_meta_data for {gaarsdagensdato}:............{str(pp_hull)}"
    konsumenter_summary = f"""
*Leste meldinger fra konsumenter siste døgn:*
 
```
{pp_antall_meldinger}
{pp_hull_i_meta_data}
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
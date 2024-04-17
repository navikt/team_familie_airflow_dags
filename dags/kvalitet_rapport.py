from datetime import datetime
from datetime import date
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_error, slack_info
from utils.db.oracle_conn import oracle_conn
from allowlists.allowlist import prod_oracle_slack, dev_oracle_slack

miljo = Variable.get('miljo')   
allowlist = []
if miljo == 'Prod':
    allowlist.extend(prod_oracle_slack)
else:
    allowlist.extend(dev_oracle_slack)

with DAG(
  dag_id='datakvalitetsrapport',
  default_args={'on_failure_callback': slack_error},
  start_date=datetime(2023, 9, 27),
  schedule_interval= "0 7 * * *", # kl 7 hver dag
  catchup=False
) as dag:

  @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
        }
    )
  def hent_kafka_last():
    # Husk å gi rettighet i riktig db, e.g. "GRANT SELECT, INSERT, UPDATE ON DVH_FAM_FP.FP_ENGANGSSTONAD_DVH TO DVH_FAM_Airflow;"
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
    bs_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_HM.brillestonad WHERE lastet_dato >= sysdate - 1
    """
    fp_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1
    """
    fp_fagsak_ant_mottatt_mldinger = """
      SELECT COUNT(DISTINCT TRANS_ID) FROM DVH_FAM_FP.FAM_FP_FAGSAK WHERE LASTET_DATO > TRUNC(SYSDATE)
    """
    es_ant_mottatt_mldinger = """ 
      SELECT COUNT(*) FROM DVH_FAM_FP.FP_ENGANGSSTONAD_DVH WHERE LASTET_DATO > TRUNC(SYSDATE)
    """ 
    sp_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_FP.FAM_SP_FAGSAK WHERE LASTET_DATO > TRUNC(SYSDATE)
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
        where neste-kafka_offset > 1 and lastet_dato > to_date('05.03.2024', 'dd.mm.yyyy')
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
    sjekk_hull_i_FP_meta_data = """
        SELECT * FROM
            (SELECT lastet_dato, kafka_topic, kafka_offset,
                LEAD(kafka_offset) 
                OVER(PARTITION BY kafka_topic
                ORDER BY kafka_offset) neste
            FROM DVH_FAM_FP.fam_fp_meta_data)
        where neste-kafka_offset > 1
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
        bs_ant = cur.execute(bs_ant_mottatt_mldinger).fetchone()[0]
        fp_ant = cur.execute(fp_ant_mottatt_mldinger).fetchone()[0]               
        fp_hull = [str(x) for x in (cur.execute(sjekk_hull_i_FP_meta_data).fetchone() or [])]  
        fp_faksak_ant = cur.execute(fp_fagsak_ant_mottatt_mldinger).fetchone()[0]  
        es_ant = cur.execute(es_ant_mottatt_mldinger).fetchone()[0]   
        sp_ant = cur.execute(sp_ant_mottatt_mldinger).fetchone()[0]   
    return [bt_ant,bt_hull,ef_ant,ef_hull,ks_ant,ks_hull,pp_ant,pp_hull,bs_ant,fp_ant,fp_hull,fp_faksak_ant,es_ant,sp_ant]


  @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
        }
    )
  def info_slack(kafka_last):
    gaarsdagensdato = date.today() - timedelta(days = 1)
    [
      bt_ant,bt_hull,
      ef_ant,ef_hull,
      ks_ant,ks_hull,
      pp_ant,pp_hull,
      bs_ant,
      fp_ant,fp_hull,    
      fp_fagsak_ant, 
      es_ant,
      sp_ant,
    ] = kafka_last
    bt_antall_meldinger = f"Antall mottatt BT meldinger for {gaarsdagensdato}......................{str(bt_ant)}"
    bt_hull_i_meta_data = f"Manglene kafka_offset i BT_meta_data for {gaarsdagensdato}:............{str(bt_hull)}"
    ef_antall_meldinger = f"Antall mottatt EF meldinger for {gaarsdagensdato}......................{str(ef_ant)}"
    ef_hull_i_meta_data = f"Manglene kafka_offset i EF_meta_data for {gaarsdagensdato}:............{str(ef_hull)}"
    ks_antall_meldinger = f"Antall mottatt KS meldinger for {gaarsdagensdato}......................{str(ks_ant)}"
    ks_hull_i_meta_data = f"Manglene kafka_offset i KS_meta_data for {gaarsdagensdato}:............{str(ks_hull)}"
    pp_antall_meldinger = f"Antall mottatt PP meldinger for {gaarsdagensdato}......................{str(pp_ant)}"
    pp_hull_i_meta_data = f"Manglene kafka_offset i PP_meta_data for {gaarsdagensdato}:............{str(pp_hull)}"
    bs_antall_meldinger = f"Antall mottatt BS meldinger for {gaarsdagensdato}......................{str(bs_ant)}"
    fp_antall_meldinger = f"Antall mottatt FP meldinger for {gaarsdagensdato}......................{str(fp_ant)}"
    fp_hull_i_meta_data = f"Manglene kafka_offset i FP_meta_data for {gaarsdagensdato}:............{str(fp_hull)}"
    fp_fagsak_antall_meldinger = f"Antall mottatt FP Fagsak meldinger for {gaarsdagensdato}...............{str(fp_fagsak_ant)}" # Fjernet 7 "." for å formatere likt med andre linjer med kortere string
    es_antall_meldinger = f"Antall mottatt ES meldinger for {gaarsdagensdato}......................{str(es_ant)}"
    sp_antall_meldinger = f"Antall mottatt SP meldinger for {gaarsdagensdato}......................{str(sp_ant)}"
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
{bs_antall_meldinger}
{fp_antall_meldinger}
{fp_hull_i_meta_data}
{fp_fagsak_antall_meldinger}
{es_antall_meldinger}
{sp_antall_meldinger}
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
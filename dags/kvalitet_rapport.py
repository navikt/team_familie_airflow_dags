from datetime import datetime
from datetime import date
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from kubernetes import client
from operators.slack_operator import slack_error, slack_info
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
    miljo = 'dev' # Har her ingen verdi, så ønsker å sette verdi for å bruke direkte i string i rapport

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
  # Husk å gi rettighet i riktig db, e.g. "GRANT SELECT, INSERT, UPDATE ON DVH_FAM_FP.FP_ENGANGSSTONAD_DVH TO DVH_FAM_Airflow;"
  def hent_kafka_last():
    bt_md_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_BT.fam_bt_meta_data WHERE lastet_dato >= sysdate - 1
    """
    ef_md_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_EF.fam_ef_meta_data WHERE lastet_dato >= sysdate - 1
    """
    ks_md_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_KS.fam_ks_meta_data WHERE lastet_dato >= sysdate - 1
    """
    pp_md_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_PP.fam_pp_meta_data WHERE lastet_dato >= sysdate - 1
    """
    bs_bs_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_HM.brillestonad WHERE lastet_dato >= sysdate - 1
    """
    # Sum av meldinger fra FP, ES & SP
    fp_md_sum_ant_mottatt_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1
    """
    fp_md_ant_mottatt_mldinger = """
      SELECT COUNT (*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE KAFKA_MOTTATT_DATO > TRUNC(SYSDATE-1) and ytelse_type = 'FORELDREPENGER';
    """
    es_md_ant_mottatt_mldinger = """
      SELECT COUNT (*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE KAFKA_MOTTATT_DATO > TRUNC(SYSDATE-1) and ytelse_type = 'ENGANGSSTØNAD';
    """    
    sp_md_ant_mottatt_mldinger = """
      SELECT COUNT (*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE KAFKA_MOTTATT_DATO > TRUNC(SYSDATE-1) and ytelse_type = 'SVANGERSKAPSPENGER';
    """
    fp_fgsk_ant_mottatt_mldinger = """
      SELECT COUNT(DISTINCT TRANS_ID) FROM DVH_FAM_FP.FAM_FP_FAGSAK WHERE LASTET_DATO > TRUNC(SYSDATE)
    """
    es_dvh_ant_mottatt_mldinger = """ 
      SELECT COUNT(*) FROM DVH_FAM_FP.FP_ENGANGSSTONAD_DVH WHERE LASTET_DATO > TRUNC(SYSDATE)
    """ 
    sp_fgsk_ant_mottatt_mldinger = """
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
        bt_md_ant = cur.execute(bt_md_ant_mottatt_mldinger).fetchone()[0]
        bt_hull = [str(x) for x in (cur.execute(sjekk_hull_i_BT_meta_data).fetchone() or [])]
        ef_md_ant = cur.execute(ef_md_ant_mottatt_mldinger).fetchone()[0]
        ef_hull = [str(x) for x in (cur.execute(sjekk_hull_i_EF_meta_data).fetchone() or [])]
        ks_md_ant = cur.execute(ks_md_ant_mottatt_mldinger).fetchone()[0]
        ks_hull = [str(x) for x in (cur.execute(sjekk_hull_i_KS_meta_data).fetchone() or [])]
        pp_md_ant = cur.execute(pp_md_ant_mottatt_mldinger).fetchone()[0]
        pp_hull = [str(x) for x in (cur.execute(sjekk_hull_i_PP_meta_data).fetchone() or [])]  
        fp_md_sum_ant = cur.execute(fp_md_sum_ant_mottatt_mldinger).fetchone()[0]               
        fp_hull = [str(x) for x in (cur.execute(sjekk_hull_i_FP_meta_data).fetchone() or [])]  

        fp_md_ant = cur.execute(fp_md_ant_mottatt_mldinger).fetchone()[0]  
        es_md_ant = cur.execute(es_md_ant_mottatt_mldinger).fetchone()[0]   
        sp_md_ant = cur.execute(sp_md_ant_mottatt_mldinger).fetchone()[0]

        fp_fgsk_ant = cur.execute(fp_fgsk_ant_mottatt_mldinger).fetchone()[0]  
        es_dvh_ant = cur.execute(es_dvh_ant_mottatt_mldinger).fetchone()[0]   
        sp_fgsk_ant = cur.execute(sp_fgsk_ant_mottatt_mldinger).fetchone()[0]

        bs_bs_ant = cur.execute(bs_bs_ant_mottatt_mldinger).fetchone()[0]
    return [bt_md_ant,bt_hull,ef_md_ant,ef_hull,ks_md_ant,ks_hull,pp_md_ant,pp_hull,fp_md_sum_ant,fp_hull,fp_md_ant,es_md_ant,sp_md_ant,fp_fgsk_ant,es_dvh_ant,sp_fgsk_ant,bs_bs_ant]


  @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
        }
    )
  def info_slack(kafka_last):
    bt_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22fll%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-barnetrygd-vedtak-v2%5C%22%7D+%3E+0+%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*BT meldinger*>"
    ef_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22od5%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-ensligforsorger-vedtak-v1%5C%22%7D%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*EF meldinger*>"
    pp_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%226xn%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22k9saksbehandling.aapen-k9-stonadstatistikk-v1%5C%22%7D%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*PP meldinger*>"
    ks_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22bmi%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-kontantstotte-vedtak-v1%5C%22%7D+%3E+0+%22%2C%22refId%22%3A%22A%22%2C%22editorMode%22%3A%22code%22%2C%22range%22%3Atrue%2C%22instant%22%3Atrue%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*KS meldinger*>"
    fp_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22bmi%22:%7B%22datasource%22:%22000000021%22,%22queries%22:%5B%7B%22exemplar%22:true,%22expr%22:%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamforeldrepenger.fpsak-dvh-stonadsstatistikk-v1%5C%22%7D%20%3E%200%20%22,%22refId%22:%22A%22,%22editorMode%22:%22code%22,%22range%22:true,%22instant%22:true,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22000000021%22%7D%7D%5D,%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D%7D&orgId=1|*FP meldinger*>"
    gaarsdagensdato = date.today() - timedelta(days = 1)
    [
      bt_md_ant,bt_hull,
      ef_md_ant,ef_hull,
      ks_md_ant,ks_hull,
      pp_md_ant,pp_hull,
      fp_md_sum_ant,fp_hull,    
      fp_md_ant,
      es_md_ant,
      sp_md_ant,
      fp_fgsk_ant, 
      es_dvh_ant,
      sp_fgsk_ant,    
      bs_ant,
    ] = kafka_last
    bt_md_antall_meldinger = f"Antall mottatt {bt_grafana} for {gaarsdagensdato}......................{str(bt_md_ant)}"
    bt_hull_i_meta_data = f"Manglene kafka_offset i BT_meta_data for {gaarsdagensdato}:............{str(bt_hull)}"
    ef_md_antall_meldinger = f"Antall mottatt {ef_grafana} for {gaarsdagensdato}......................{str(ef_md_ant)}"
    ef_hull_i_meta_data = f"Manglene kafka_offset i EF_meta_data for {gaarsdagensdato}:............{str(ef_hull)}"
    ks_md_antall_meldinger = f"Antall mottatt {ks_grafana} for {gaarsdagensdato}......................{str(ks_md_ant)}"
    ks_hull_i_meta_data = f"Manglene kafka_offset i KS_meta_data for {gaarsdagensdato}:............{str(ks_hull)}"
    pp_md_antall_meldinger = f"Antall mottatt {pp_grafana} for {gaarsdagensdato}......................{str(pp_md_ant)}"
    pp_hull_i_meta_data = f"Manglene kafka_offset i PP_meta_data for {gaarsdagensdato}:............{str(pp_hull)}"
    fp_md_sum_antall_meldinger = f"Antall mottatt {fp_grafana} for {gaarsdagensdato}......................{str(fp_md_sum_ant)}"
    fp_hull_i_meta_data = f"Manglene kafka_offset i FP_meta_data for {gaarsdagensdato}:............{str(fp_hull)}"

    fp_md_antall_meldinger = f"Antall mottatt FP meldinger fra meta_data for {gaarsdagensdato}..................{str(fp_md_ant)}" 
    ef_md_antall_meldinger = f"Antall mottatt ES meldinger fra meta_data for {gaarsdagensdato}..................{str(es_md_ant)}"
    sp_md_antall_meldinger = f"Antall mottatt SP meldinger fra meta_data for {gaarsdagensdato}..................{str(sp_md_ant)}"

    fp_fgsk_antall_meldinger = f"Antall mottatt FP meldinger fra fagsak for {gaarsdagensdato}..................{str(fp_fgsk_ant)}" 
    es_dvh_antall_meldinger = f"Antall mottatt ES meldinger fra ENGANGSSTONAD_DVH for {gaarsdagensdato}..................{str(es_dvh_ant)}"
    sp_fgsk_antall_meldinger = f"Antall SP meldinger fra meta_data for {gaarsdagensdato}..................{str(sp_fgsk_ant)}"

    bs_bs_antall_meldinger = f"Antall mottatt BS meldinger for {gaarsdagensdato}......................{str(bs_bs_ant)}"

    konsumenter_summary = f"""
*Leste {miljo} meldinger fra konsumenter siste døgn:*
 
```
{bs_bs_antall_meldinger}
{pp_md_antall_meldinger}
{pp_hull_i_meta_data}
{bt_md_antall_meldinger}
{bt_hull_i_meta_data}
{ef_md_antall_meldinger}
{ef_hull_i_meta_data}
{ks_md_antall_meldinger}
{ks_hull_i_meta_data}
{fp_md_sum_antall_meldinger}
{fp_hull_i_meta_data}

{fp_md_antall_meldinger}
{ef_md_antall_meldinger}
{sp_md_antall_meldinger}

{fp_fgsk_antall_meldinger}
{es_dvh_antall_meldinger}
{sp_fgsk_antall_meldinger}
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
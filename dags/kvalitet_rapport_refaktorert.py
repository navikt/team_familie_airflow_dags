from datetime import datetime, timedelta
import datetime as dt
import pytz
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
elif miljo == 'R':
    allowlist.extend(r_oracle_slack)   
else:
    allowlist.extend(dev_oracle_slack)
    miljo = 'dev'  # For formateringsformål

#TODO: Dette kan forenkles i fremtiden, eller legges i en funksjon
# Definer ansvarlige per uke for 2026
uke = [i for i in range(1, 54)]
ansvarlig = ["Arafa", "Gard", "Hans", "Helen/Gard"]
# Lag ansvarlig-listen ved å repetere ansvarlig-listen og stoppe etter 53 elementer (tilsvarende uker i året)
antall_uker = len(uke)
gjentatt = (ansvarlig * ((antall_uker // len(ansvarlig)) + 1))[:antall_uker]
ansvarlig = gjentatt
ansvarlig_per_uke = dict(zip(uke, ansvarlig))

def hent_ansvarlig_per_uke(uke):
    """Return ansvarlig for gitt uke eller None hvis ikke definert."""
    return ansvarlig_per_uke.get(uke)

with DAG(
    dag_id='Dagsrapport_v2',
    description = 'Daglig rapport over meldingsantall fra konsumenter i Team Familie DVH',
    default_args={'on_failure_callback': slack_error},
    start_date=datetime(2024, 10, 9),
    schedule_interval="0 23 * * *", # 0 CET
    catchup=False,
) as dag:

    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(allowlist)})
            )
        }
    )
    # Husk å grante rettigheter ved bruk av nye konsumenter, e.g. "GRANT SELECT ON DVH_FAM_BB.fam_bb_meta_data TO DVH_FAM_Airflow;"
    def fetch_kafka_counts():
        count_queries = {
            "bb_count_md": "SELECT COUNT(*) FROM DVH_FAM_BB.fam_bb_meta_data WHERE lastet_dato >= sysdate - 1",
            "bb_count_fg": "SELECT COUNT(*) FROM DVH_FAM_BB.FAM_BB_FAGSAK WHERE lastet_dato >= sysdate - 1",
            "bb_count_fg_ord": "SELECT COUNT(*) FROM DVH_FAM_BB.FAM_BB_FAGSAK_ORD WHERE lastet_dato >= sysdate - 1",
            "bt_count": "SELECT COUNT(*) FROM DVH_FAM_BT.fam_bt_meta_data WHERE lastet_dato >= sysdate - 1",
            "ef_count": "SELECT COUNT(*) FROM DVH_FAM_EF.fam_ef_meta_data WHERE lastet_dato >= sysdate - 1",
            "ts_count_v2": "SELECT COUNT(*) FROM DVH_FAM_EF.fam_ts_meta_data_v2 WHERE lastet_dato >= sysdate - 1", #v2
            "ts_fgsk_count_v2": "SELECT COUNT(DISTINCT ekstern_behandling_id) FROM DVH_FAM_EF.fam_ts_fagsak_v2 WHERE lastet_dato >= sysdate - 1", #v2
            "ks_count": "SELECT COUNT(*) FROM DVH_FAM_KS.fam_ks_meta_data WHERE lastet_dato >= sysdate - 1",
            "pp_count": "SELECT COUNT(*) FROM DVH_FAM_PP.fam_pp_meta_data WHERE lastet_dato >= sysdate - 1",
            "bs_count": "SELECT COUNT(*) FROM DVH_FAM_HM.brillestonad WHERE lastet_dato >= sysdate - 1",
            "fp_sum_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1",
            "fp_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1 AND ytelse_type = 'FORELDREPENGER'",
            "es_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1 AND ytelse_type = 'ENGANGSSTØNAD'",
            "sp_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1 AND ytelse_type = 'SVANGERSKAPSPENGER'",
        }

        with oracle_conn().cursor() as cur:
            result = {}
            for key, query in count_queries.items():
                count = cur.execute(query).fetchone()[0]
                result[key] = count
            return result
    
    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(allowlist)})
            )
        }
    )
    def check_for_gaps():
        gap_queries = {
            "sjekk_hull_i_BB_meta_data_forskudd": """
                SELECT * FROM
                    (SELECT lastet_dato, kafka_topic, kafka_offset,
                        LEAD(kafka_offset) 
                        OVER(PARTITION BY kafka_topic
                        ORDER BY kafka_offset) neste
                    FROM DVH_FAM_BB.fam_bb_meta_data
                    WHERE TYPE_STONAD = 'FORSKUDD')
                WHERE neste - kafka_offset > 1
            """, 
            "sjekk_hull_i_BB_meta_data_bidrag": """
                SELECT * FROM
                    (SELECT lastet_dato, kafka_topic, kafka_offset,
                        LEAD(kafka_offset) 
                        OVER(PARTITION BY kafka_topic
                        ORDER BY kafka_offset) neste
                    FROM DVH_FAM_BB.fam_bb_meta_data
                    WHERE TYPE_STONAD = 'BIDRAG')
                WHERE neste - kafka_offset > 1 AND kafka_offset > 1650726
            """, 
            "sjekk_hull_i_BT_meta_data": """
                SELECT * FROM
                    (SELECT lastet_dato, kafka_topic, kafka_offset,
                        LEAD(kafka_offset) 
                        OVER(PARTITION BY kafka_topic
                        ORDER BY kafka_offset) neste
                    FROM DVH_FAM_BT.fam_bt_meta_data)
                WHERE kafka_offset > 766801 AND neste - kafka_offset > 1
            """,
            "sjekk_hull_i_EF_meta_data": """
                SELECT * FROM
                    (SELECT lastet_dato, kafka_topic, kafka_offset,
                        LEAD(kafka_offset) 
                        OVER(PARTITION BY kafka_topic
                        ORDER BY kafka_offset) neste
                    FROM DVH_FAM_EF.fam_ef_meta_data)
                WHERE lastet_dato > TO_DATE('01.08.2023', 'dd.mm.yyyy') AND neste - kafka_offset > 1
            """,
            "sjekk_hull_i_KS_meta_data": """
                SELECT * FROM
                    (SELECT lastet_dato, kafka_topic, kafka_offset,
                        LEAD(kafka_offset) 
                        OVER(PARTITION BY kafka_topic
                        ORDER BY kafka_offset) neste
                    FROM DVH_FAM_KS.fam_ks_meta_data)
                WHERE neste - kafka_offset > 1 AND lastet_dato > TO_DATE('05.03.2024', 'dd.mm.yyyy')
            """,
            "sjekk_hull_i_PP_meta_data": """
                SELECT * FROM
                    (SELECT lastet_dato, kafka_topic, kafka_offset,
                        LEAD(kafka_offset) 
                        OVER(PARTITION BY kafka_topic
                        ORDER BY kafka_offset) neste
                    FROM DVH_FAM_PP.fam_pp_meta_data)
                WHERE neste - kafka_offset > 1 AND lastet_dato > TO_DATE('24.10.2023', 'dd.mm.yyyy')
            """,
            "sjekk_hull_i_FP_meta_data": """
                SELECT * FROM
                    (SELECT lastet_dato, kafka_topic, kafka_offset, kafka_partition,
                        LEAD(kafka_offset) 
                        OVER(PARTITION BY kafka_topic, kafka_partition 
                        ORDER BY kafka_offset) neste
                    FROM DVH_FAM_FP.fam_fp_meta_data
                    WHERE kafka_mottatt_dato > TO_DATE('16.04.2024','dd.mm.yyyy'))
                WHERE neste - kafka_offset > 1
            """
        }

        with oracle_conn().cursor() as cur:
            result = {}
            for key, query in gap_queries.items():
                gap_result = [str(x) for x in cur.execute(query).fetchone() or []]
                result[key] = gap_result
            return result
        
    @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(allowlist)})
            )
        }
    )
    def info_slack(kafka_last, gaps):
        # Hardkodede Grafana-lenker
        bb_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%226xn%22:%7B%22datasource%22:%22000000021%22,%22queries%22:%5B%7B%22exemplar%22:true,%22expr%22:%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22bidrag.statistikk%5C%22%7D%20%3E%200%22,%22refId%22:%22A%22,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22000000021%22%7D,%22editorMode%22:%22code%22,%22range%22:true,%22instant%22:true%7D%5D,%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D%7D&orgId=1|*BB meldinger*>"
        bt_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22fll%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-barnetrygd-vedtak-v2%5C%22%7D+%3E+0+%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*BT meldinger*>"
        ef_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22od5%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-ensligforsorger-vedtak-v1%5C%22%7D%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*EF meldinger*>"
        pp_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%226xn%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22k9saksbehandling.aapen-k9-stonadstatistikk-v1%5C%22%7D%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*PP meldinger*>"
        ks_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22bmi%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-kontantstotte-vedtak-v1%5C%22%7D+%3E+0+%22%2C%22refId%22%3A%22A%22%2C%22editorMode%22%3A%22code%22%2C%22range%22%3Atrue%2C%22instant%22%3Atrue%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*KS meldinger*>"
        fp_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22bmi%22:%7B%22datasource%22:%22000000021%22,%22queries%22:%5B%7B%22exemplar%22:true,%22expr%22:%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamforeldrepenger.fpsak-dvh-stonadsstatistikk-v1%5C%22%7D%20%3E%200%20%22,%22refId%22:%22A%22,%22editorMode%22:%22code%22,%22range%22:true,%22instant%22:true,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22000000021%22%7D%7D%5D,%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D%7D&orgId=1|*FP meldinger*>"
        
        dagensdato = dt.datetime.now(pytz.timezone("Europe/Oslo")) # Henter dagens dato i Oslo tidssone, automatisk sommertidshåndtering
        gaarsdagensdato = dagensdato - dt.timedelta(days=1) # Trekker fra en dag for å få gårsdagens dato
        gaarsdagensdato = gaarsdagensdato.strftime("%Y-%m-%d %H:%M:%S") # Formaterer vekk millisekund
        hentet_uke = dagensdato.isocalendar()[1]  # Henter uke nummer (fikset veriabel)
        hentet_ansvarlig = hent_ansvarlig_per_uke(hentet_uke) or "Ukjent" #Hent ansvarlig per uke

        # Hver linje statisk opprettet, letteste løsning når det er flere forskjeller i hver string
        bb_count_str = f"Antall mottatt {bb_grafana} i meta data/fagsak + ord.......{str(kafka_last['bb_count_md'])}/{str(kafka_last['bb_count_fg'])}/{str(kafka_last['bb_count_fg_ord'])}"
        bs_count_str = f"Antall mottatt BS meldinger................................{str(kafka_last['bs_count'])}"
        pp_count_str = f"Antall mottatt {pp_grafana}................................{str(kafka_last['pp_count'])}"
        bt_count_str = f"Antall mottatt {bt_grafana}................................{str(kafka_last['bt_count'])}"
        ef_count_str = f"Antall mottatt {ef_grafana}................................{str(kafka_last['ef_count'])}"
        ts_count_math_str = f"Antall mottatt totale/pakket ut i fagsak TS v2 meldinger...{str(kafka_last['ts_count_v2'])}/{str(kafka_last['ts_fgsk_count_v2'])}"
        ks_count_str = f"Antall mottatt {ks_grafana}................................{str(kafka_last['ks_count'])}"
        fp_sum_count_str = f"Antall mottatt summerte {fp_grafana}.......................{str(kafka_last['fp_sum_count'])}"
        fp_count_str = f"Antall mottatt FP meldinger................................{str(kafka_last['fp_count'])}" 
        es_count_str = f"Antall mottatt ES meldinger................................{str(kafka_last['es_count'])}"
        sp_count_str = f"Antall mottatt SP meldinger................................{str(kafka_last['sp_count'])}"

        # Stringen må formateres sånn som dette for å se riktig ut, se bort fra merkelig tab indent i IDE
        konsumenter_summary = f"""
*Dagsrapport*
Ansvarlig denne uken (uke {hentet_uke}): {hentet_ansvarlig}
Leste {miljo} meldinger fra konsumenter siden {gaarsdagensdato}:

```
{bb_count_str}
{bs_count_str}
{pp_count_str}
{bt_count_str}
{ef_count_str}
{ts_count_math_str}
{ks_count_str}
{fp_sum_count_str}
{fp_count_str}
{es_count_str}
{sp_count_str}
```
"""
        
        # Send rapporten til Slack, sjekker etter hull senere
        slack_info(
            message=konsumenter_summary,
            emoji=":newspaper:"
        )

        # Sjekker etter hull
        bb_hull_forskudd = gaps.get("sjekk_hull_i_BB_meta_data_forskudd")
        bb_hull_bidrag = gaps.get("sjekk_hull_i_BB_meta_data_bidrag")
        bt_hull = gaps.get("sjekk_hull_i_BT_meta_data")
        ef_hull = gaps.get("sjekk_hull_i_EF_meta_data")
        ks_hull = gaps.get("sjekk_hull_i_KS_meta_data")
        pp_hull = gaps.get("sjekk_hull_i_PP_meta_data")
        fp_hull = gaps.get("sjekk_hull_i_FP_meta_data")

        # Hvis noen topics inneholder hull, konkatineres navn på topic med komma mellomrom hvert navn
        topics_med_hull = ", ".join(str(sublist) for sublist in [bb_hull_forskudd, bb_hull_bidrag, bt_hull, ef_hull, ks_hull, pp_hull, fp_hull] if sublist)

        # Sjekker om noe ble lagt til i string, hvis ikke sendes annen string
        if topics_med_hull:
            # Høy prioritering hvis hull oppdages, ønsker å gjøre alle oppmerk på dette med "!channel"
            notification_summary = (f"```<!channel> Minst ett hull oppdaget i topic {topics_med_hull}, sjekk manuelt for flere!```")
            slack_info(
                message=notification_summary,
                emoji=":newspaper:"
            )
        else:
            notification_summary = (f"```Ingen hull oppdaget i noen av våre topics.```")
            slack_info(
                message=notification_summary,
                emoji=":newspaper:"
            )

    kafka_last = fetch_kafka_counts()
    check_gaps = check_for_gaps()
    post_til_info_slack = info_slack(kafka_last, check_gaps)

    kafka_last >> check_gaps >> post_til_info_slack
"""
Dagsrapport_v2
---------------
Daglig rapport over meldingsantall fra konsumenter i Team Familie DVH til meta data.
- Henter aggregerte antall siste døgn.
- Sjekker om det er hull (manglende kafka_offset) i flere topics.
- Poster rapport og ev. varsel til Slack.

NB:
 - Husk å gi nødvendige database-rettigheter ved nye konsumenter, f.eks.:
   GRANT SELECT ON DVH_FAM_BB.fam_bb_meta_data TO DVH_FAM_AIRFLOW;
"""

from datetime import timedelta
import pendulum
from typing import Dict, List, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from kubernetes import client

from operators.slack_operator import slack_error, slack_info
from utils.db.oracle_conn import oracle_conn
from allowlists.allowlist import prod_oracle_slack, dev_oracle_slack, r_oracle_slack


# Miljø og allowlist for pod-annotasjon (slik at databasen åpnes for riktig IP)
miljo = Variable.get("miljo")
allowlist: List[str] = []

if miljo == "Prod":
    allowlist.extend(prod_oracle_slack)
elif miljo == "R":
    allowlist.extend(r_oracle_slack)
else:
    allowlist.extend(dev_oracle_slack)
    miljo = "dev"  # Primært for visning/formattering i Slack

OSLO_TZ = pendulum.timezone("Europe/Oslo")


def hent_ansvarlig_per_uke(uke: int) -> Optional[str]:
    """
    Returnerer ansvarlig for gitt uke eller None hvis ikke definert.
    """
    uke_liste = list(range(1, 54))  # 53 uker er maks for ISO-uker
    ansvarlige = ["Arafa", "Gard", "Hans", "Helen/Gard"]
    gjentatt = (ansvarlige * ((len(uke_liste) // len(ansvarlige)) + 1))[: len(uke_liste)]
    ansvarlig_per_uke = dict(zip(uke_liste, gjentatt))
    return ansvarlig_per_uke.get(uke)


with DAG(
    dag_id="Dagsrapport_v3",
    description="Daglig rapport over meldingsantall fra konsumenter i Team Familie DVH",
    default_args={
        "on_failure_callback": slack_error,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=pendulum.datetime(2026, 1, 15, tz=OSLO_TZ),
    schedule="0 0 * * *",  # Lokal midnatt i Oslo
    catchup=False,
) as dag:

    common_executor_config = {
        "pod_override": client.V1Pod(
            metadata=client.V1ObjectMeta(annotations={"allowlist": ",".join(allowlist)})
        )
    }

    @task(executor_config=common_executor_config)
    def fetch_kafka_counts() -> Dict[str, int]:
        """
        Henter antall meldinger siste døgn per konsument/tabell.
        """
        count_queries: Dict[str, str] = {
            "up_count": "SELECT COUNT(*) FROM DVH_FAM_UNGDOM.fam_UP_meta_data WHERE lastet_dato >= sysdate - 1",
            "bb_count_md": "SELECT COUNT(*) FROM DVH_FAM_BB.fam_bb_meta_data WHERE lastet_dato >= sysdate - 1",
            "bb_count_fg": "SELECT COUNT(*) FROM DVH_FAM_BB.FAM_BB_FAGSAK WHERE lastet_dato >= sysdate - 1",
            "bb_count_fg_ord": "SELECT COUNT(*) FROM DVH_FAM_BB.FAM_BB_FAGSAK_ORD WHERE lastet_dato >= sysdate - 1",
            "bt_count": "SELECT COUNT(*) FROM DVH_FAM_BT.fam_bt_meta_data WHERE lastet_dato >= sysdate - 1",
            "ef_count": "SELECT COUNT(*) FROM DVH_FAM_EF.fam_ef_meta_data WHERE lastet_dato >= sysdate - 1",
            "ts_count_v2": "SELECT COUNT(*) FROM DVH_FAM_EF.fam_ts_meta_data_v2 WHERE lastet_dato >= sysdate - 1",
            "ts_fgsk_count_v2": "SELECT COUNT(DISTINCT ekstern_behandling_id) FROM DVH_FAM_EF.fam_ts_fagsak_v2 WHERE lastet_dato >= sysdate - 1",
            "ks_count": "SELECT COUNT(*) FROM DVH_FAM_KS.fam_ks_meta_data WHERE lastet_dato >= sysdate - 1",
            "pp_count": "SELECT COUNT(*) FROM DVH_FAM_PP.fam_pp_meta_data WHERE lastet_dato >= sysdate - 1",
            "bs_count": "SELECT COUNT(*) FROM DVH_FAM_HM.brillestonad WHERE lastet_dato >= sysdate - 1",
            "fp_sum_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1",
            "fp_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1 AND ytelse_type = 'FORELDREPENGER'",
            "es_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1 AND ytelse_type = 'ENGANGSSTØNAD'",
            "sp_count": "SELECT COUNT(*) FROM DVH_FAM_FP.FAM_FP_META_DATA WHERE lastet_dato >= sysdate - 1 AND ytelse_type = 'SVANGERSKAPSPENGER'",
        }

        result: Dict[str, int] = {}
        with oracle_conn().cursor() as cur:
            for key, query in count_queries.items():
                result[key] = int(cur.execute(query).fetchone()[0])
        return result

    @task(executor_config=common_executor_config)
    def check_for_gaps() -> Dict[str, int]:
        """
        Sjekker om det finnes "hull" i kafka_offset pr. topic.
        """
        gap_queries: Dict[str, str] = {
            "UP meta_data": """
                SELECT COUNT(*) FROM (
                    SELECT kafka_offset,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_UNGDOM.fam_up_meta_data
                )
                WHERE neste - kafka_offset > 1
                  AND kafka_offset > 1059
            """,
            "BB meta_data (forskudd)": """
                SELECT COUNT(*) FROM (
                    SELECT kafka_offset,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_BB.fam_bb_meta_data
                    WHERE TYPE_STONAD = 'FORSKUDD'
                )
                WHERE neste - kafka_offset > 1
            """,
            "BB meta_data (bidrag)": """
                SELECT COUNT(*) FROM (
                    SELECT kafka_offset,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_BB.fam_bb_meta_data
                    WHERE TYPE_STONAD = 'BIDRAG'
                )
                WHERE neste - kafka_offset > 1
                  AND kafka_offset > 1650726
            """,
            "BT meta_data": """
                SELECT COUNT(*) FROM (
                    SELECT kafka_offset,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_BT.fam_bt_meta_data
                )
                WHERE kafka_offset > 766801
                  AND neste - kafka_offset > 1
            """,
            "EF meta_data": """
                SELECT COUNT(*) FROM (
                    SELECT lastet_dato, kafka_offset,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_EF.fam_ef_meta_data
                )
                WHERE lastet_dato > TO_DATE('01.08.2023', 'dd.mm.yyyy')
                  AND neste - kafka_offset > 1
            """,
            "KS meta_data": """
                SELECT COUNT(*) FROM (
                    SELECT lastet_dato, kafka_offset,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_KS.fam_ks_meta_data
                )
                WHERE neste - kafka_offset > 1
                  AND lastet_dato > TO_DATE('05.03.2024', 'dd.mm.yyyy')
            """,
            "PP meta_data": """
                SELECT COUNT(*) FROM (
                    SELECT lastet_dato, kafka_offset,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_PP.fam_pp_meta_data
                )
                WHERE neste - kafka_offset > 1
                  AND lastet_dato > TO_DATE('24.10.2023', 'dd.mm.yyyy')
            """,
            "FP meta_data": """
                SELECT COUNT(*) FROM (
                    SELECT kafka_offset, kafka_partition,
                           LEAD(kafka_offset) OVER (PARTITION BY kafka_topic, kafka_partition ORDER BY kafka_offset) AS neste
                    FROM DVH_FAM_FP.fam_fp_meta_data
                    WHERE kafka_mottatt_dato > TO_DATE('16.04.2024','dd.mm.yyyy')
                )
                WHERE neste - kafka_offset > 1
            """,
        }

        result: Dict[str, int] = {}
        with oracle_conn().cursor() as cur:
            for key, query in gap_queries.items():
                result[key] = int(cur.execute(query).fetchone()[0])
        return result

    @task(executor_config=common_executor_config)
    def info_slack(kafka_last: Dict[str, int], gaps: Dict[str, int]) -> None:
        """
        Sender dagsrapport til slack, pluss varsel om hull hvis oppdaget. 
        """
        # Hardkodede Grafana-lenker, format <url|visningstekst>. Er midlertidig fjernet til fordel for Bookmarks i Slack, pga 4000 slack melding krav. Den formaterte meldingen ble sendt over flere meldinger, som gjorde at den mistet formatet sitt. 
        #up_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%2237b%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22k9saksbehandling.aapen-ung-stonadstatistikk-v1%5C%22%7D+%3E+0+%22%2C%22refId%22%3A%22A%22%2C%22editorMode%22%3A%22code%22%2C%22range%22%3Atrue%2C%22instant%22%3Atrue%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*UP meldinger*>"
        #bb_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%226xn%22:%7B%22datasource%22:%22000000021%22,%22queries%22:%5B%7B%22exemplar%22:true,%22expr%22:%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22bidrag.statistikk%5C%22%7D%20%3E%200%22,%22refId%22:%22A%22,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22000000021%22%7D,%22editorMode%22:%22code%22,%22range%22:true,%22instant%22:true%7D%5D,%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D%7D&orgId=1|*BB meldinger*>"
        #bt_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22fll%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-barnetrygd-vedtak-v2%5C%22%7D+%3E+0+%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*BT meldinger*>"
        #ef_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22od5%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-ensligforsorger-vedtak-v1%5C%22%7D%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*EF meldinger*>"
        #pp_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%226xn%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22k9saksbehandling.aapen-k9-stonadstatistikk-v1%5C%22%7D%22%2C%22refId%22%3A%22A%22%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*PP meldinger*>"
        #ks_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22bmi%22%3A%7B%22datasource%22%3A%22000000021%22%2C%22queries%22%3A%5B%7B%22exemplar%22%3Atrue%2C%22expr%22%3A%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamfamilie.aapen-kontantstotte-vedtak-v1%5C%22%7D+%3E+0+%22%2C%22refId%22%3A%22A%22%2C%22editorMode%22%3A%22code%22%2C%22range%22%3Atrue%2C%22instant%22%3Atrue%2C%22datasource%22%3A%7B%22type%22%3A%22prometheus%22%2C%22uid%22%3A%22000000021%22%7D%7D%5D%2C%22range%22%3A%7B%22from%22%3A%22now-1h%22%2C%22to%22%3A%22now%22%7D%7D%7D&orgId=1|*KS meldinger*>"
        #fp_grafana = "<https://grafana.nav.cloud.nais.io/explore?schemaVersion=1&panes=%7B%22bmi%22:%7B%22datasource%22:%22000000021%22,%22queries%22:%5B%7B%22exemplar%22:true,%22expr%22:%22kafka_log_Log_LogEndOffset_Value%7Btopic%3D%5C%22teamforeldrepenger.fpsak-dvh-stonadsstatistikk-v1%5C%22%7D%20%3E%200%20%22,%22refId%22:%22A%22,%22editorMode%22:%22code%22,%22range%22:true,%22instant%22:true,%22datasource%22:%7B%22type%22:%22prometheus%22,%22uid%22:%22000000021%22%7D%7D%5D,%22range%22:%7B%22from%22:%22now-1h%22,%22to%22:%22now%22%7D%7D%7D&orgId=1|*FP meldinger*>"

        now_oslo = pendulum.now(OSLO_TZ)
        yesterday = now_oslo.subtract(days=1).replace(microsecond=0)
        hentet_uke = now_oslo.isocalendar()[1]
        hentet_ansvarlig = hent_ansvarlig_per_uke(hentet_uke) or "Ukjent" # Henter ansvarlig per uke

        # Linjer i rapporten, statisk opprettet
        up_count_str = f"Antall mottatt UP meldinger................................{kafka_last['up_count']}"
        bb_count_str = f"Antall mottatt BB meldinger i meta/fagsak/ord..............{kafka_last['bb_count_md']}/{kafka_last['bb_count_fg']}/{kafka_last['bb_count_fg_ord']}"
        bs_count_str = f"Antall mottatt BS meldinger................................{kafka_last['bs_count']}"
        pp_count_str = f"Antall mottatt PP meldinger................................{kafka_last['pp_count']}"
        bt_count_str = f"Antall mottatt BT meldinger................................{kafka_last['bt_count']}"
        ef_count_str = f"Antall mottatt EF meldinger................................{kafka_last['ef_count']}"
        ts_count_math_str = f"Antall mottatt TS v2 total/fagsak.........................{kafka_last['ts_count_v2']}/{kafka_last['ts_fgsk_count_v2']}"
        ks_count_str = f"Antall mottatt KS meldinger................................{kafka_last['ks_count']}"
        fp_sum_count_str = f"Antall mottatt summerte FP meldinger.......................{kafka_last['fp_sum_count']}"
        fp_count_str = f"Antall mottatt FP meldinger................................{kafka_last['fp_count']}"
        es_count_str = f"Antall mottatt ES meldinger................................{kafka_last['es_count']}"
        sp_count_str = f"Antall mottatt SP meldinger................................{kafka_last['sp_count']}"

        # Se bort fra merkelig tab ident i IDE, må formateres slik for å se riktig ut
        konsumenter_summary = f"""
*Dagsrapport*
Ansvarlig denne uken (uke {hentet_uke}): {hentet_ansvarlig}
Leste {miljo} meldinger siden {yesterday.to_datetime_string()}:
```
{up_count_str}
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

        slack_info(message=konsumenter_summary, emoji=":newspaper:")

        # Sjekk hull
        topics_med_hull = [navn for navn, antall in gaps.items() if antall > 0]

        # Sjekker om noe ble lagt til i string, hvis ikke sendes else string
        if topics_med_hull:
            slack_info(
                message=f"<!channel> Det er oppdaget hull i følgende: {', '.join(topics_med_hull)}. Sjekk manuelt for flere! :rotating_light:",
                emoji=":rotating_light:",
            )
        else:
            slack_info(
                message="Ingen hull oppdaget i noen topics. :green_check_mark:",
                emoji=":green_check_mark:",
            )

    kafka_last = fetch_kafka_counts()
    check_gaps = check_for_gaps()
    post_til_info_slack = info_slack(kafka_last, check_gaps)

    kafka_last >> check_gaps >> post_til_info_slack
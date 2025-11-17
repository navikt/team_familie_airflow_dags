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
    miljo = 'dev' # Miljø er aldri dev, men ønsker å sette verdi for å bruke direkte i string i rapport

with DAG(
  dag_id='Ukesrapport',
  description = 'Ukentlig rapport av verdier Team Familie DVH ikke trenger å sjekke daglig',
  default_args={'on_failure_callback': slack_error},
  start_date=datetime(2024, 9, 2),
  schedule_interval="0 23 * * 1", # 0 CET, hver mandag. Pass på å ikke kjøre samtidig med noen konsumenter, vil rapportere upakkede meldinger
  catchup=False
) as dag:

  @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
        }
    )    
  # Ved bruk av nye db, husk å gi rettighet til airflow, e.g. "GRANT SELECT ON DVH_FAM_FP.FP_ENGANGSSTONAD_DVH TO DVH_FAM_Airflow;"
  def hent_kafka_last():
    # Antall kode67 søker PSB meldinger ikke pakket ut
    kode67_soker = """
    SELECT
        COUNT(1)
    FROM
        (
            SELECT
                JSON_VALUE(melding, '$.søker') soker_fnr,
                trunc(CAST(TO_TIMESTAMP_TZ(JSON_VALUE(meta.melding, '$.vedtakstidspunkt'),
                    'yyyy-mm-dd"T"hh24:mi:ss.ff3') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP),
                      'dd')                    vedtaks_dag,
                kafka_offset,
                kafka_partition,
                ident.skjermet_kode
            FROM
                    dvh_fam_pp.fam_pp_meta_data meta
                INNER JOIN dt_person.ident_off_id_til_fk_person1 ident ON JSON_VALUE(meta.melding, '$.søker') = ident.off_id
                                                                          AND trunc(CAST(TO_TIMESTAMP_TZ(JSON_VALUE(meta.melding, '$.vedtakstidspunkt'
                                                                          ),
                    'yyyy-mm-dd"T"hh24:mi:ss.ff3') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP),
                                                                                    'dd') BETWEEN gyldig_fra_dato AND gyldig_til_dato
                                                                          AND ident.skjermet_kode IN ( 6, 7 )
            WHERE
                    meta.lastet_dato >= sysdate - 7
                AND JSON_VALUE(meta.melding, '$.ytelseType') = 'PSB'
        )
    """
    # Antall kode67 pleietrengende PSB meldinger ikke pakket ut
    kode67_pleietrengende = """
    SELECT
        COUNT(1)
    FROM
        (
            SELECT
                JSON_VALUE(melding, '$.pleietrengende') pleietrengende_fnr,
                trunc(CAST(TO_TIMESTAMP_TZ(JSON_VALUE(meta.melding, '$.vedtakstidspunkt'),
                    'yyyy-mm-dd"T"hh24:mi:ss.ff3') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP),
                      'dd')                             vedtaks_dag,
                kafka_offset,
                kafka_partition,
                ident.skjermet_kode
            FROM
                    dvh_fam_pp.fam_pp_meta_data meta
                INNER JOIN dt_person.ident_off_id_til_fk_person1 ident ON JSON_VALUE(meta.melding, '$.pleietrengende') = ident.off_id
                                                                          AND trunc(CAST(TO_TIMESTAMP_TZ(JSON_VALUE(meta.melding, '$.vedtakstidspunkt'
                                                                          ),
                    'yyyy-mm-dd"T"hh24:mi:ss.ff3') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP),
                                                                                    'dd') BETWEEN gyldig_fra_dato AND gyldig_til_dato
                                                                          AND ident.skjermet_kode IN ( 6, 7 )
            WHERE
                    meta.lastet_dato >= sysdate - 7
                AND JSON_VALUE(meta.melding, '$.ytelseType') = 'PSB'
        )
    """
    # Antall FP meldinger ikke pakket ut
    fp_ikke_pakket_ut = """
    SELECT
        COUNT(1) antall
    FROM
        dvh_fam_fp.fam_fp_meta_data   meta
        LEFT OUTER JOIN dvh_fam_fp.json_fam_fp_fagsak fagsak ON JSON_VALUE(meta.melding, '$.behandlingUuid') = fagsak.behandling_uuid
    WHERE
        fagsak.behandling_uuid IS NULL
        AND trunc(meta.lastet_dato, 'dd') >= trunc(sysdate, 'dd') - 7
    """
    # FP ny inntektskategori
    fp_ny_inntektskategori = """
    SELECT
        utbet.inntektskategori
    FROM
        (
            SELECT DISTINCT
                inntektskategori
            FROM
                dvh_fam_fp.json_fam_fp_utbetalingsperioder
        ) utbet
        LEFT OUTER JOIN (
            SELECT DISTINCT
                inntektskategori
            FROM
                dvh_fam_fp.test_mapping
        ) mapping ON utbet.inntektskategori = mapping.inntektskategori
    WHERE
        mapping.inntektskategori IS NULL
    """
    # FP ny aktivitet
    fp_ny_aktivitet = """
    SELECT
        bereg.andeler_aktivitet
    FROM
        (
            SELECT DISTINCT
                andeler_aktivitet
            FROM
                dvh_fam_fp.json_fam_fp_beregning
        ) bereg
        LEFT OUTER JOIN (
            SELECT DISTINCT
                aktivitet
            FROM
                dvh_fam_fp.test_mapping
        ) mapping ON bereg.andeler_aktivitet = mapping.aktivitet
    WHERE
        mapping.aktivitet IS NULL
    """
    pp_nye_arbeidsforhold = """
    select distinct arbeidsforhold_type
        from dvh_fam_pp.fam_pp_periode_utbet_grader
            where dagsats > 0
        and lastet_dato > sysdate - 8
        and arbeidsforhold_type not in
        (
        select distinct arbeidsforhold_type
            from dvh_fam_pp.fam_pp_aktivitet_underkonto_mapping
        )
    """
    # Antall TS meldinger patched siste uken
    ts_md_ant_patchede_mldinger = """
      SELECT COUNT(*) FROM DVH_FAM_EF.fam_ts_meta_data_v2 WHERE trunc(opprettet_tid,'Mi') != trunc(endret_tid,'Mi') AND endret_tid >= sysdate - 7
    """

    with oracle_conn().cursor() as cur:
        kode67_soker_ant = cur.execute(kode67_soker).fetchone()[0]
        kode67_pleie_ant = cur.execute(kode67_pleietrengende).fetchone()[0]
        fp_ikke_pakket_ut_ant = cur.execute(fp_ikke_pakket_ut).fetchone()[0]
        fp_ny_inntektskategori_ant = [str(x) for x in (cur.execute(fp_ny_inntektskategori).fetchone() or [])]
        fp_ny_aktivitet_ant = [str(x) for x in (cur.execute(fp_ny_aktivitet).fetchone() or [])]
        pp_nye_arbeidsforhold_ant = [str(x) for x in (cur.execute(pp_nye_arbeidsforhold).fetchone() or [])]
        ts_md_ant_patchede_mldinger_ant = cur.execute(ts_md_ant_patchede_mldinger).fetchone()[0]

    # MÅ stå i riktig rekkefølge!
    return [
        kode67_soker_ant,
        kode67_pleie_ant,
        fp_ikke_pakket_ut_ant,
        fp_ny_inntektskategori_ant,
        fp_ny_aktivitet_ant,
        pp_nye_arbeidsforhold_ant,
        ts_md_ant_patchede_mldinger_ant,
    ]


  @task(
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
        }
    )
  def info_slack(kafka_last):
    # Dato for nøyaktig tidspunkt en uke siden
    forrigeuke = dt.datetime.now(pytz.timezone("Europe/Oslo")) - dt.timedelta(days=7)
    #forrigeuke = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=2) - dt.timedelta(days=7)
    forrigeuke = forrigeuke.strftime("%Y-%m-%d %H:%M:%S") # Formaterer vekk millisekund, blir for mye informasjon i rapporten
    
    #gaarsdagensdato = dt.datetime.now(pytz.timezone("Europe/Oslo")) - dt.timedelta(days=1) # Henter gårsdagen i Oslo tidssone, automatisk sommertidshåndtering
    #gaarsdagensdato = gaarsdagensdato.strftime("%Y-%m-%d %H:%M:%S") # Formaterer vekk millisekund
    [
      kode67_soker_ant,
      kode67_pleie_ant,
      fp_ikke_pakket_ut_ant,
      fp_ny_inntektskategori_ant,
      fp_ny_aktivitet_ant,
      pp_nye_arbeidsforhold_ant,
      ts_md_ant_patchede_mldinger_ant,
    ] = kafka_last
    soker_antall_string = f"Antall kode67 søker PSB meldinger ikke pakket ut............{str(kode67_soker_ant)}"
    pleie_antall_string = f"Antall kode67 pleietrengende PSB meldinger ikke pakket ut...{str(kode67_pleie_ant)}"
    fp_antall_string = f"Antall FP meldinger ikke pakket ut..........................{str(fp_ikke_pakket_ut_ant)}"
    fp_ny_inntektskategori_string = f"FP ny inntektskategori......................................{str(fp_ny_inntektskategori_ant)}"
    fp_ny_aktivitet_string = f"FP ny aktivitet.............................................{str(fp_ny_aktivitet_ant)}"
    pp_nye_arbeidsforhold_string = f"PP nye arbeidsforhold.......................................{str(pp_nye_arbeidsforhold_ant)}"
    ts_md_ant_patchede_mldinger_string = f"Antall TS meldinger v2 patched..............................{str(ts_md_ant_patchede_mldinger_ant)}"
    konsumenter_summary = f"""
*Ukesrapport*
Leste {miljo} meldinger fra konsumenter siden {forrigeuke}:
 
```
{soker_antall_string}
{pleie_antall_string}
{fp_antall_string}
{fp_ny_inntektskategori_string}
{fp_ny_aktivitet_string}
{pp_nye_arbeidsforhold_string}
{ts_md_ant_patchede_mldinger_string}
```
"""
    # Slack melding med antall meldinger
    slack_info(
      message=f"{konsumenter_summary}",
      emoji=":newspaper:"
    )

  kafka_last = hent_kafka_last()
  post_til_info_slack = info_slack(kafka_last)

  kafka_last >> post_til_info_slack
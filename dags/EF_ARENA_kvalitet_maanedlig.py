from datetime import datetime
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
  dag_id='ef_arena_kvalitet_maanedlig',
  default_args={'on_failure_callback': slack_error},
  start_date=datetime(2024, 3, 21),
  schedule_interval= "0 12 5 * *", # kl 12 den 5. hver måned
  catchup=True
) as dag:
   @task(
       executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist":  ",".join(allowlist)})
            )
       }
   )
   
   def diff():
      diff_fak_slutt_tabell = """  
        with arena as
        (
        select periode, maalgruppe_kode, maalgruppe_navn, stonad_kode
                ,count(distinct case when belop is not null then fk_person1 end) ant_fk_person1
        from fam_ef_stonad_arena
        unpivot(
            belop
            for stonad_kode
            in (
                tsoboutg as 'TSOBOUTG',
                tsodagreis as 'TSODAGREIS',
                tsoflytt as 'TSOFLYTT',
                tsolmidler as 'TSOLMIDLER',
                tsoreisakt as 'TSOREISAKT',
                tsoreisarb as 'TSOREISARB',
                tsoreisobl as 'TSOREISOBL',
                tsotilbarn as 'TSOTILBARN',
                tsotilfam as 'TSOTILFAM'
            ))
        group by periode, maalgruppe_kode, maalgruppe_navn, stonad_kode
        ),
        fak as
        (
        select to_char(fs.postert_dato, 'yyyymm') periode, maalgruppe_kode, stonad_kode, stonad_omraade_fin_navn
                ,count(distinct fk_person1) ant_fk_person1
        from dt_p.fak_stonad fs
        
        join dt_p.dim_f_stonad_omraade so
        on fs.fk_dim_f_stonad_omraade = so.pk_dim_f_stonad_omraade
        and so.stonad_kode in ('TSOBOUTG','TSODAGREIS','TSOFLYTT','TSOLMIDLER','TSOREISAKT', 'TSOREISARB','TSOREISOBL','TSOTILBARN','TSOTILFAM')
        
        join dt_p.dim_maalgruppe_type maalt
        on fs.fk_dim_maalgruppe_type = maalt.pk_dim_maalgruppe_type
        and maalt.maalgruppe_kode in ('ENSFORUTD','ENSFORARBS','TIDLFAMPL','GJENEKUTD','GJENEKARBS')
        
        join dt_p.dim_vedtak_postering vp
        on fs.fk_dim_vedtak_postering = vp.pk_dim_vedtak_postering
        
        group by to_char(fs.postert_dato, 'yyyymm'), maalgruppe_kode, stonad_kode, stonad_omraade_fin_navn
        order by to_char(fs.postert_dato, 'yyyymm'), stonad_kode, maalgruppe_kode, stonad_omraade_fin_navn
        ),
        diff as
        (
        select fak.*, arena.ant_fk_person1 ant_fk_person1_arena
                ,fak.ant_fk_person1-arena.ant_fk_person1 as diff_ant_fk_person1
        from fak
        left join arena
        on fak.periode = arena.periode
        and fak.maalgruppe_kode = arena.maalgruppe_kode
        and fak.stonad_kode = arena.stonad_kode
        )
        select *
        from diff
        where diff_ant_fk_person1 > 0
        and periode = 202401
      """
      with oracle_conn().cursor() as cur:
        diff = [str(x) for x in (cur.execute(diff_fak_slutt_tabell).fetchone() or [])]   
      return [diff]
  
   def info_slack(diff):
     slack_info(
      message=f"{diff}",
      emoji=":newspaper:"
     )

   finn_ut_diff = diff()
   post_til_info_slack = info_slack(finn_ut_diff)
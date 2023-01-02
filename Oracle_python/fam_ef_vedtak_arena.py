from utils.db.oracle_conn import oracle_conn
from felles_metoder import get_periode

def vedtak_arena_delete_insert():
    periode = get_periode()

    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'delete/insert into dvh_fam_ef.fam_ef_vedtak_arena' );
        end;
    ''')

    delete_periode_sql = (f"delete from dvh_fam_ef.fam_ef_vedtak_arena where periode = {periode}")

    insert_data_sql = ('''
            INSERT INTO dvh_fam_ef.fam_ef_vedtak_arena (FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
            ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
            ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO)
            SELECT FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
            ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
            ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO
            FROM dvh_fam_ef.ef_vedtak_arena
        ''')

    with oracle_conn():
        with oracle_conn().cursor() as cur:
            cur.execute(send_context_sql)
            cur.execute(delete_periode_sql)
            cur.execute(insert_data_sql)   
    oracle_conn().close()

if __name__ == "__main__":
    vedtak_arena_delete_insert()



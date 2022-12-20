from utils.db.oracle_conn import oracle_conn, oracle_conn_close
from felles_metoder import get_periode, send_context

# def delete_data(conn, cur, periode):
#     """
#     sletter data fra fam_ef_stonad_arena med periode som kriteriea.
#     :param periode:
#     :return:
#     """
#     sql = (f"delete from dvh_fam_ef.fam_ef_vedtak_arena where periode = {periode}")
#     cur.execute(sql)
#     conn.commit()

# def insert_data(conn, cur):
#     """
#     insert data fra ef_stonad_arena_final (view laget med dbt) into fam_ef_stonad_arena
#     :param:
#     :return:
#     """
#     sql = ('''
#             INSERT INTO dvh_fam_ef.fam_ef_vedtak_arena (FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
#             ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
#             ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO)
#             SELECT FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
#             ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
#             ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO
#             FROM dvh_fam_ef.ef_vedtak_arena
#         ''')
#     cur.execute(sql)
#     conn.commit()


def vedtak_arena_delete_insert():
    periode = get_periode

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

    with oracle_conn().cursor() as cur:
        cur.execute(send_context_sql)
        cur.execute(delete_periode_sql)
        cur.execute(insert_data_sql)   


if __name__ == "__main__":
    vedtak_arena_delete_insert()
    # periode = get_periode()
    # conn, cur = oracle_conn()
    # action_name = 'delete/insert into dvh_fam_ef.fam_ef_vedtak_arena'
    # send_context(conn, cur, action_name)
    # delete_data(conn, cur, periode)
    # insert_data(conn, cur)
    # oracle_conn_close(conn)


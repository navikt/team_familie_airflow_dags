from felles_metoder import set_secrets_as_envs, get_periode, send_context
from utils.db.oracle_conn import oracle_conn, oracle_conn_close

# def delete_data(conn, cur, periode):
#     """
#     sletter data fra fam_ef_stonad_arena med periode som kriteriea.
#     :param periode:
#     :return:
#     """
#     sql = (f"delete from dvh_fam_ef.fam_ef_stonad_arena where periode = {periode}")
#     cur.execute(sql)
#     conn.commit()

# def insert_data(conn, cur):
#     """
#     insert data fra ef_stonad_arena_final (view laget med dbt) into fam_ef_stonad_arena
#     :param:
#     :return:
#     """
#     sql = ('''
#             INSERT INTO dvh_fam_ef.fam_ef_stonad_arena (FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
#             ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
#             ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
#             ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI)
#             SELECT FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
#             ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
#             ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
#             ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI
#             FROM dvh_fam_ef.ef_stonad_arena_final
#         ''')
#     cur.execute(sql)
#     conn.commit()


def stonad_arena_delete_insert():
    periode = get_periode

    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'delete/insert into dvh_fam_ef.fam_ef_stonad_arena' );
        end;
    ''')

    delete_periode_sql = (f"delete from dvh_fam_ef.fam_ef_stonad_arena where periode = {periode}")

    insert_data_sql = ('''
            INSERT INTO dvh_fam_ef.fam_ef_stonad_arena (FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
            ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
            ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
            ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI)
            SELECT FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
            ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
            ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
            ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI
            FROM dvh_fam_ef.ef_stonad_arena_final
        ''')

    with oracle_conn().cursor() as cur:
        cur.execute(send_context_sql)
        cur.execute(delete_periode_sql)
        cur.execute(insert_data_sql)


if __name__ == "__main__":
    stonad_arena_delete_insert()
    #periode = get_periode()
    #conn, cur = oracle_conn()
    #action_name = 'delete/insert into dvh_fam_ef.fam_ef_stonad_arena'
    #send_context(conn, cur, action_name)
    #delete_data(conn, cur, periode)
    #insert_data(conn, cur)
    #oracle_conn_close(conn)


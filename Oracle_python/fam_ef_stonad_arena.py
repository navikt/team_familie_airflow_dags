import datetime
from utils.db.oracle_conn import oracle_conn

def get_periode():
    """
    henter periode for the tidligere måneden eksample--> i dag er 19.04.2022, metoden vil kalkulerer periode aarMaaned eks) '202203'
    :param periode:
    :return: periode
    """
    today = datetime.date.today() # dato for idag 2022-04-19
    first = today.replace(day=1) # dato for første dag i måneden 2022-04-01
    lastMonth = first - datetime.timedelta(days=1) # dato for siste dag i tidligere måneden

    return lastMonth.strftime("%Y%m") # henter bare aar og maaned

def stonad_arena_delete_insert():
    periode = get_periode()

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

    conn = oracle_conn()
    with conn.cursor() as cur:
        cur.execute(send_context_sql)
        cur.execute(delete_periode_sql)
        cur.execute(insert_data_sql)
        conn.commit()    

if __name__ == "__main__":
    stonad_arena_delete_insert()



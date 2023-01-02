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

def patch_ybarn_arena():
    periode = get_periode()

    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'Patcher ybarn til fam_ef_stonad' );
        end;
    ''')

    sql = (f"""
            DECLARE
                P_IN_PERIODE_YYYYMM NUMBER;
                P_OUT_ERROR VARCHAR2(200);
    
            BEGIN
            P_IN_PERIODE_YYYYMM := {periode};
            P_OUT_ERROR := NULL;
            FAM_EF.fam_ef_patch_infotrygd_arena (  P_IN_PERIODE_YYYYMM => P_IN_PERIODE_YYYYMM,P_OUT_ERROR => P_OUT_ERROR) ;  
            END;
        """)

    conn = oracle_conn()
    with oracle_conn().cursor() as cur:
        cur.execute(send_context_sql)
        cur.execute(sql)
        conn.commit()

if __name__ == "__main__":
    patch_ybarn_arena()


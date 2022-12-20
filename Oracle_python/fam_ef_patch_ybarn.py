from felles_metoder import set_secrets_as_envs, get_periode
from utils.db.oracle_conn import oracle_conn

def patch_ybarn_arena():
    set_secrets_as_envs()
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

    with oracle_conn.cursor() as cur:
        cur.execute(send_context_sql)
        cur.execute(sql)

if __name__ == "__main__":
    patch_ybarn_arena()


import cx_Oracle
from felles_metoder.felles_metoder import oracle_secrets, get_periode

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
    
    secrets = oracle_secrets()
  
    dsn_tns = cx_Oracle.makedsn(secrets['host'], 1521, service_name = secrets['service'])
    with cx_Oracle.connect(user = secrets['user'], password = secrets['password'], dsn = dsn_tns) as connection:
        with connection.cursor() as cursor:
            cursor.execute(send_context_sql)
            cursor.execute(sql)
            connection.commit()

if __name__ == "__main__":
    patch_ybarn_arena()


import cx_Oracle
from felles_metoder.felles_metoder import oracle_secrets, get_periode

def patch_fk_person1():
    periode = get_periode()

    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'Patcher fk_person1' );
        end;
    ''')
    
    secrets = oracle_secrets()

    dsn_tns = cx_Oracle.makedsn(secrets['host'], 1521, service_name = secrets['service'])
    with cx_Oracle.connect(user = secrets['user']+'[dvh_fam_bt]', password = secrets['password'], dsn = dsn_tns) as connection:
        with connection.cursor() as cursor:
            cursor.execute(send_context_sql)
            
            cursor.callproc('FAM_BT_SKEDULERING.fam_bt_patch_fk_person1', [periode])
            
            connection.commit()

if __name__ == "__main__":
    patch_fk_person1()
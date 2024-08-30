import oracledb
from felles_metoder.felles_metoder import oracle_secrets, get_periode

def fam_skedulering():

    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'Skedulere familie pakke som ligger i oracle' );
        end;
    ''')
    
    secrets = oracle_secrets()

    dsn_tns = oracledb.makedsn(secrets['host'], 1521, service_name = secrets['service'])
    with oracledb.connect(user = secrets['user']+'[dvh_fam_fp]', password = secrets['password'], dsn = dsn_tns) as connection:
        with connection.cursor() as cursor:
            cursor.execute(send_context_sql)     
                 
            cursor.callproc('fam_skedulering.fam_skedulering', [None, None])

            connection.commit()

if __name__ == "__main__":
    fam_skedulering()
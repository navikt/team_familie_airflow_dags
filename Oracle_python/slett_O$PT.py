import oracledb
from felles_metoder.felles_metoder import oracle_secrets

def delete_OSPT():
    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'Sletter O$PT fra alle skjemaer' );
        end;
    ''')

    secrets = oracle_secrets()
    dsn_tns = oracledb.makedsn(secrets['host'], 1521, service_name = secrets['service'])

    skjemaer = ["DVH_FAM_PP", "DVH_FAM_EF", "DVH_FAM_BT", "DVH_FAM_KS", "DVH_FAM_FP"] # Alt annet enn DVH_FAM_HM, da den ikke bruker DBT
    for skjema in skjemaer:
        with oracledb.connect(user = f"{secrets['user']}[{skjema}]", password = secrets['password'], dsn = dsn_tns) as connection:
            with connection.cursor() as cursor:
                cursor.execute(send_context_sql)
                cursor.execute(f"""
                                BEGIN  FOR t in (SELECT table_name as tname FROM user_tables WHERE table_name like 'O$PT%')
                                    LOOP
                                        EXECUTE immediate 'drop table ' || t.tname;
                                    END LOOP;
                                    EXCEPTION  WHEN OTHERS THEN
                                            IF SQLCODE != -942 THEN
                                                RAISE;
                                            END IF;
                                    END;
                                    /
                                COMMIT; 
                            """)
                connection.commit()

if __name__ == "__main__":
    delete_OSPT()              
  
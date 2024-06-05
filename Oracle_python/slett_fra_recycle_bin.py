import oracledb
from felles_metoder.felles_metoder import oracle_secrets

def delete_from_recycle_bin():
    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'sletter dbt temp tabeller fra recyle bin' );
        end;
    ''')

    secrets = oracle_secrets()

    dsn_tns = oracledb.makedsn(secrets['host'], 1521, service_name = secrets['service'])

    skjemaer = ["dvh_fam_pp", "dvh_fam_ef", "dvh_fam_bt", "dvh_fam_ks", "dvh_fam_fp"]

    for skjema in skjemaer:
        with oracledb.connect(user = f"{secrets['user']}[{skjema}]", password = secrets['password'], dsn = dsn_tns) as connection:
            with connection.cursor() as cursor:
                cursor.execute(send_context_sql)
                cursor.execute(f"""BEGIN FOR rec IN (SELECT object_name, original_name FROM  dba_recyclebin WHERE type = 'TABLE' AND OWNER={skjema} AND ORIGINAL_NAME LIKE '%DBT%') LOOP
                            EXECUTE IMMEDIATE 'PURGE TABLE "' || rec.object_name || '"'; END LOOP; END;""")
                connection.commit()

if __name__ == "__main__":
    delete_from_recycle_bin()              
  
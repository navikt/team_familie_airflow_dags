import cx_Oracle
from felles_metoder.felles_metoder import oracle_secrets, get_periode

def delete_from_recycle_bin():
    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'sletter dbt temp tabeller fra recyle bin' );
        end;
    ''')

    delete_ef_tables_sql = (f"BEGIN FOR rec IN (SELECT object_name, original_name FROM  dba_recyclebin WHERE type = 'TABLE' AND OWNER='DVH_FAM_EF' AND ORIGINAL_NAME LIKE '%DBT%') LOOP
                            EXECUTE IMMEDIATE 'PURGE TABLE "' || rec.object_name || '"'; END LOOP; END;/")
    
    delete_pp_tables_sql = (f"BEGIN FOR rec IN (SELECT object_name, original_name FROM  dba_recyclebin WHERE type = 'TABLE' AND OWNER='DVH_FAM_PP' AND ORIGINAL_NAME LIKE '%DBT%') LOOP
                        EXECUTE IMMEDIATE 'PURGE TABLE "' || rec.object_name || '"'; END LOOP; END;/")
    
    delete_bt_tables_sql = (f"BEGIN FOR rec IN (SELECT object_name, original_name FROM  dba_recyclebin WHERE type = 'TABLE' AND OWNER='DVH_FAM_BT' AND ORIGINAL_NAME LIKE '%DBT%') LOOP
                        EXECUTE IMMEDIATE 'PURGE TABLE "' || rec.object_name || '"'; END LOOP; END;/")
    
    delete_ks_tables_sql = (f"BEGIN FOR rec IN (SELECT object_name, original_name FROM  dba_recyclebin WHERE type = 'TABLE' AND OWNER='DVH_FAM_KS' AND ORIGINAL_NAME LIKE '%DBT%') LOOP
                        EXECUTE IMMEDIATE 'PURGE TABLE "' || rec.object_name || '"'; END LOOP; END;/")
    
    delete_fp_tables_sql = (f"BEGIN FOR rec IN (SELECT object_name, original_name FROM  dba_recyclebin WHERE type = 'TABLE' AND OWNER='DVH_FAM_FP' AND ORIGINAL_NAME LIKE '%DBT%') LOOP
                            EXECUTE IMMEDIATE 'PURGE TABLE "' || rec.object_name || '"'; END LOOP; END;/")

    
    secrets = oracle_secrets()

    dsn_tns = cx_Oracle.makedsn(secrets['host'], 1521, service_name = secrets['service'])
    with cx_Oracle.connect(user = secrets['user'], password = secrets['password'], dsn = dsn_tns) as connection:
        with connection.cursor() as cursor:
            cursor.execute(send_context_sql)
            cursor.execute(delete_ef_tables_sql)
            cursor.execute(delete_pp_tables_sql)
            cursor.execute(delete_bt_tables_sql)
            cursor.execute(delete_ks_tables_sql)
            cursor.execute(delete_fp_tables_sql)
            connection.commit()
  
if __name__ == "__main__":
    delete_from_recycle_bin()
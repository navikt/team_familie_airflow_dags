import oracledb
from felles_metoder import oracle_secrets, get_periode

def vedtak_arena_delete_insert():
    periode = get_periode()

    send_context_sql = (f'''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'delete/insert into dvh_fam_ef.fam_ef_vedtak_arena' );
        end;
    ''')

    delete_periode_sql = (f"delete from dvh_fam_ef.fam_ef_vedtak_arena where periode = {periode}")

    insert_data_sql = ('''
            INSERT INTO dvh_fam_ef.fam_ef_vedtak_arena (FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
            ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
            ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO)
            SELECT FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
            ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
            ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO
            FROM dvh_fam_ef.ef_vedtak_arena
            ''')

    secrets = oracle_secrets()

    dsn_tns = oracledb.makedsn(secrets['host'], 1521, service_name = secrets['service'])
    with oracledb.connect(user = secrets['user'], password = secrets['password'], dsn = dsn_tns) as connection:
        with connection.cursor() as cursor:
            cursor.execute(send_context_sql)
            #cursor.execute(delete_periode_sql)
            cursor.execute(insert_data_sql)
            connection.commit()

if __name__ == "__main__":
    vedtak_arena_delete_insert()
    



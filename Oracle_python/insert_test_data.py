import cx_Oracle
from felles_metoder.felles_metoder import oracle_secrets

def opprett_oracle_tabell():
    secrets = oracle_secrets()

    insert_data_sql = ("""INSERT INTO dvh_fam_ef.test_tabell (id, navn)
                        VALUES (1,  'Kjell-Tore')
                        """)

    dsn_tns = cx_Oracle.makedsn(secrets['host'], 1521, service_name = secrets['service'])
    with cx_Oracle.connect(user = secrets['user']['dvh_fam_ef'], password = secrets['password'], dsn = dsn_tns) as connection:
        with connection.cursor() as cursor:
            cursor.execute(insert_data_sql)
            connection.commit()


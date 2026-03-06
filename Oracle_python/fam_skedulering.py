import logging
import oracledb
from felles_metoder.felles_metoder import oracle_secrets

logger = logging.getLogger(__name__)

def fam_skedulering():

    send_context_sql = '''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'Skedulere familie pakke som ligger i oracle' );
        end;
    '''

    secrets = oracle_secrets()

    try:
        dsn_tns = oracledb.makedsn(secrets['host'], 1521, service_name=secrets['service'])

        with oracledb.connect(user=secrets['user']+'[dvh_fam_fp]', password=secrets['password'], dsn=dsn_tns) as connection:
            with connection.cursor() as cursor:

                logger.info("Sending Oracle context")

                cursor.execute(send_context_sql)

                logger.info("Calling procedure fam_skedulering.fam_skedulering")

                cursor.callproc('fam_skedulering.fam_skedulering', [None, None])

                connection.commit()

                logger.info("Procedure completed successfully")

    except oracledb.DatabaseError as e:
        error, = e.args
        logger.error("Oracle error code: %s", error.code)
        logger.error("Oracle error message: %s", error.message)
        raise

if __name__ == "__main__":
    fam_skedulering()
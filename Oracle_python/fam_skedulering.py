import logging
import oracledb
from felles_metoder.felles_metoder import oracle_secrets

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  

def fam_skedulering():
    """
    Calls the Oracle procedure FAM_SKEDULERING.fam_skedulering
    and ensures any internal errors are captured and logged.
    """

    # SQL to set Oracle client/module context
    send_context_sql = '''
        BEGIN
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module(module_name => 'Kjører Team-familie Airflow applikasjon',
                action_name => 'Skedulere familie pakke som ligger i oracle');
        END;
    '''

    secrets = oracle_secrets()
    dsn_tns = oracledb.makedsn(secrets['host'], 1521, service_name=secrets['service'])

    try:
        with oracledb.connect(
            user=secrets['user'] + '[dvh_fam_fp]',
            password=secrets['password'],
            dsn=dsn_tns
        ) as connection:

            with connection.cursor() as cursor:
                logger.info("Sending Oracle context...")
                cursor.execute(send_context_sql)

                logger.info("Calling procedure fam_skedulering.fam_skedulering...")
                # Allocate a large enough buffer for p_out_error
                out_error = cursor.var(str, size=4000)

                # Call the procedure
                cursor.callproc('fam_skedulering.fam_skedulering', [None, out_error])
                connection.commit()

                # Check if the procedure internally reported an error
                if out_error.getvalue():
                    logger.error("Procedure returned error: %s", out_error.getvalue())
                    # Raise exception so Airflow task fails
                    raise Exception(f"fam_skedulering failed: {out_error.getvalue()}")
                else:
                    logger.info("Procedure completed successfully.")

    except oracledb.DatabaseError as e:
        # Catch Oracle errors 
        error, = e.args
        logger.error("Oracle error code: %s", error.code)
        logger.error("Oracle error message: %s", error.message)
        raise  # Re-raise to ensure Airflow marks the task as failed


if __name__ == "__main__":
    fam_skedulering()
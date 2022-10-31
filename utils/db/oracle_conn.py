import cx_Oracle
from os import getenv
from dataverk_vault.api import set_secrets_as_envs

def oracle_secrets():
  set_secrets_as_envs()
  return dict(
    user=getenv("AIRFLOW_ORCL_USER"),
    password=getenv("AIRFLOW_ORCL_PASS"),
    host = getenv("DBT_ORCL_HOST"),
    service = getenv("DBT_ORCL_SERVICE"),
    encoding="UTF-8",
    nencoding="UTF-8"
  )

oracle_secrets = oracle_secrets()

def oracle_conn():
    conn = None
    cur = None
    dsn_tns = cx_Oracle.makedsn(oracle_secrets['host'], 1521, service_name = oracle_secrets['service'])

    try:
        conn = cx_Oracle.connect(user = oracle_secrets['user'], password = oracle_secrets['password'], dsn = dsn_tns)
        cur = conn.cursor()
        print(conn, cur)
        return conn, cur
    except cx_Oracle.Error as error:
        print(error)

def oracle_conn_close(conn):
    conn.close()
    print('Connection to the database was successfuly closed')

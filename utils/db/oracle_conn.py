import cx_Oracle
from os import getenv
import os, json
from google.cloud import secretmanager

def set_secrets_as_envs():
  secrets = secretmanager.SecretManagerServiceClient()
  resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
  secret = secrets.access_secret_version(name=resource_name)
  secret_str = secret.payload.data.decode('UTF-8')
  secrets = json.loads(secret_str)
  os.environ.update(secrets)

  
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

def oracle_conn():
    dsn_tns = cx_Oracle.makedsn(oracle_secrets()['host'], 1521, service_name = oracle_secrets()['service'])
    try:
        conn = cx_Oracle.connect(user = oracle_secrets()['user'], password = oracle_secrets()['password'], dsn = dsn_tns)
        print(conn)
        return conn
    except cx_Oracle.Error as error:
        print(error)


import datetime, os, json
from utils.db import oracle_conn
from google.cloud import secretmanager



def set_secrets_as_envs():
  secrets = secretmanager.SecretManagerServiceClient()
  resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
  secret = secrets.access_secret_version(name=resource_name)
  secret_str = secret.payload.data.decode('UTF-8')
  secrets = json.loads(secret_str)
  os.environ.update(secrets)

def get_periode():
    """
    henter periode for the tidligere måneden eksample--> i dag er 19.04.2022, metoden vil kalkulerer periode aarMaaned eks) '202203'
    :param periode:
    :return: periode
    """
    today = datetime.date.today() # dato for idag 2022-04-19
    first = today.replace(day=1) # dato for første dag i måneden 2022-04-01
    lastMonth = first - datetime.timedelta(days=1) # dato for siste dag i tidligere måneden

    return lastMonth.strftime("%Y%m") # henter bare aar og maaned


def patch_ybarn_arena(conn, cur, periode):
    sql = ("""
            DECLARE
                P_IN_PERIODE_YYYYMM NUMBER;
                P_OUT_ERROR VARCHAR2(200);
    
            BEGIN
            P_IN_PERIODE_YYYYMM := {};
            P_OUT_ERROR := NULL;
            FAM_EF.fam_ef_patch_infotrygd_arena (  P_IN_PERIODE_YYYYMM => P_IN_PERIODE_YYYYMM,P_OUT_ERROR => P_OUT_ERROR) ;  
            END;
        """.format(periode))

    cur.execute(sql)
    conn.commit()

def send_context(conn, cur):
    sql = ('''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'Patcher ybarn til fam_ef_stonad' );
        end;
    ''')
    cur.execute(sql)
    conn.commit()


if __name__ == "__main__":

    periode = get_periode()
    conn, cur = oracle_conn.oracle_conn()
    send_context(conn, cur)
    patch_ybarn_arena(conn, cur, periode)

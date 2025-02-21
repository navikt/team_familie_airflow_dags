from felles_metoder.felles_metoder import oracle_secrets
import oracledb, os, json

def hent_data_fra_oracle_fp():
  oracle_secrets = oracle_secrets()

  user = oracle_secrets['user'] + '[DVH_FAM_FP]'
  print(user) #Test
  dsn_tns = oracledb.makedsn(oracle_secrets['host'], 1521, service_name = oracle_secrets['service'])

  with oracledb.connect(user=user, password = oracle_secrets['password'], dsn=dsn_tns) as conn:
      with conn.cursor() as cursor:
          #sql_query = f"SELECT * FROM {oracle_view} where arstall = {year} and kvartal = {quarter}"
          #cursor.execute(sql_query)
          print(user) #Test
          conn.commit()

if __name__ == "__main__":
   hent_data_fra_oracle_fp()
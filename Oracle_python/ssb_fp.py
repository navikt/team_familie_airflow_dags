from felles_metoder.felles_metoder import oracle_secrets
import oracledb, os, json

def hent_data_fra_oracle_fp():
  print('Test1')
  oracle_info = oracle_secrets()
  print('Test2')

  user = oracle_info['user'] + '[DVH_FAM_FP]'
  print('Test3')
  print(user) #Test
  dsn_tns = oracledb.makedsn(oracle_info['host'], 1521, service_name = oracle_info['service'])
  print('Test4')

  with oracledb.connect(user=user, password = oracle_info['password'], dsn=dsn_tns) as conn:
      with conn.cursor() as cursor:
          print('Test5')
          #sql_query = f"SELECT * FROM {oracle_view} where arstall = {year} and kvartal = {quarter}"
          #cursor.execute(sql_query)
          print(user) #Test
          conn.commit()

if __name__ == "__main__":
   hent_data_fra_oracle_fp
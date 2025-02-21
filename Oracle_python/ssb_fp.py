from felles_metoder.felles_metoder import oracle_secrets
import oracledb, os, json

def hent_data_fra_oracle():
  oracle_info = oracle_secrets()

  user = oracle_info['user'] + '[DVH_FAM_FP]'
  print(user) #Test
  dsn_tns = oracledb.makedsn(oracle_info['host'], 1521, service_name = oracle_info['service'])

  with oracledb.connect(user=user, password = oracle_info['password'], dsn=dsn_tns) as conn:
      with conn.cursor() as cursor:
          sql_query = f"select count(1) from vfam_fp_sp_ssb_2024_m"
          cursor.execute(sql_query)
          print(sql_query)
          for row in cursor:
             print(row)
          conn.commit()

if __name__ == "__main__":
   hent_data_fra_oracle()
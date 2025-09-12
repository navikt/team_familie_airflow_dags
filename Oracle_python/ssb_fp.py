from felles_metoder.felles_metoder import oracle_secrets
import oracledb, os, json
from io import StringIO
import paramiko
import pandas as pd

def hent_data_fra_oracle():
    oracle_info = oracle_secrets()

    user = oracle_info['user'] + '[DVH_FAM_FP]'

    dsn_tns = oracledb.makedsn(oracle_info['host'], 1521, service_name = oracle_info['service'])

    with oracledb.connect(user=user, password = oracle_info['password'], dsn=dsn_tns) as conn:
        with conn.cursor() as cursor:
            sql_query = f"select count(1) from vfam_fp_sp_ssb_2024_m"
            print(sql_query) #Test
            #cursor.execute(sql_query)
            #Konvert resultat av sql til Pandas DataFrame
            #odf = cursor.connection.fetch_df_all(statement=sql_query, arraysize=1000)
            #df = pd.api.interchange.from_dataframe(odf)
            df = pd.read_sql(sql_query, conn)
            

            #koble til sftp:
            sftpkey = os.getenv('SFTPKEY')
            keyfile = StringIO(sftpkey)
            mykey = paramiko.RSAKey.from_private_key(keyfile, password=None)
            #Open a transport
            host,port = "a01drvl099.adeo.no",22
            transport = paramiko.Transport((host,port))
            #Auth    
            username= "srv-dv-familie-airflow-sas"
            transport.connect(username=username,pkey=mykey)

            with paramiko.SFTPClient.from_transport(transport) as sftp:
                print("connected") #Test
                print(sftp.get_channel()) #Test
                print(sftp.listdir(path='.')) #Test

                #Konvert data from database to csv format
                with open("./inbound/kildefiler/bidrag/test_fp.csv", "w") as text_file:
                    text_file.write(df.to_csv(index=False))

                # Close sftp
                if sftp: sftp.close()
                if transport: transport.close()

            # Close database connection
            conn.commit()   

if __name__ == "__main__":
   hent_data_fra_oracle()
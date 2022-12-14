import sys, os
from felles_metoder import set_secrets_as_envs, get_periode, send_context
from utils.db.oracle_conn import oracle_conn, oracle_conn_close

def patch_ybarn_arena(conn, cur, periode):
    sql = (f"""
            DECLARE
                P_IN_PERIODE_YYYYMM NUMBER;
                P_OUT_ERROR VARCHAR2(200);
    
            BEGIN
            P_IN_PERIODE_YYYYMM := {periode};
            P_OUT_ERROR := NULL;
            FAM_EF.fam_ef_patch_infotrygd_arena (  P_IN_PERIODE_YYYYMM => P_IN_PERIODE_YYYYMM,P_OUT_ERROR => P_OUT_ERROR) ;  
            END;
        """)
    cur.execute(sql)
    conn.commit()

if __name__ == "__main__":
    set_secrets_as_envs()
    periode = get_periode()
    conn, cur = oracle_conn()
    action_name = 'Patcher ybarn til fam_ef_stonad'
    send_context(conn, cur, action_name)
    patch_ybarn_arena(conn, cur, periode)
    oracle_conn_close()

from utils.db.oracle_conn import oracle_conn, oracle_conn_close
from felles_metoder import get_periode, send_context

def delete_data(conn, cur, periode):
    """
    sletter data fra fam_ef_stonad_arena med periode som kriteriea.
    :param periode:
    :return:
    """
    sql = (f"delete from dvh_fam_ef.fam_ef_vedtak_arena where periode = {periode}")
    cur.execute(sql)
    conn.commit()

def insert_data(conn, cur):
    """
    insert data fra ef_stonad_arena_final (view laget med dbt) into fam_ef_stonad_arena
    :param:
    :return:
    """
    sql = ('''
            INSERT INTO dvh_fam_ef.fam_ef_vedtak_arena (FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
            ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
            ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO)
            SELECT FK_PERSON1,PERIODE,LK_VEDTAK_ID,KOMMUNE_NR,BYDEL_NR,VEDTAK_SAK_RESULTAT_KODE,STONAD_KODE,STONADBERETT_AKTIVITET_FLAGG,AAR,SAK_STATUS_KODE,STONAD_NAVN
            ,MAALGRUPPE_KODE,MAALGRUPPE_NAVN,VEDTAK_DATO,VILKAAR_KODE,VILKAAR_STATUS_KODE,VILKAAR_NAVN,VEDTAK_SAK_TYPE_KODE,VEDTAK_BEHANDLING_STATUS
            ,GYLDIG_FRA_DATO,GYLDIG_TIL_DATO,KILDESYSTEM,LASTET_DATO
            FROM dvh_fam_ef.ef_vedtak_arena
        ''')
    cur.execute(sql)
    conn.commit()

if __name__ == "__main__":
    periode = get_periode()
    conn, cur = oracle_conn()
    action_name = 'delete/insert into dvh_fam_ef.fam_ef_vedtak_arena'
    send_context(conn, cur, action_name)
    delete_data(conn, cur, periode)
    insert_data(conn, cur)
    oracle_conn_close(conn)


def delete_data(conn, cur, periode):
    """
    sletter data fra fam_ef_stonad_arena med periode som kriteriea.
    :param periode:
    :return:
    """
    sql = ("delete from dvh_fam_ef.fam_ef_vedtak_arena where periode = {}".format(periode))
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


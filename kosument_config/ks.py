config="""
source:
  type: kafka
  batch-size: 50
  batch-interval: 5
  topic: aapen-kontantstotte-vedtak-v1
  schema: avro
target:
  type: oracle
  skip-duplicates-with: kafka_offset
  table: DVH_FAM_KS.FAM_KS_META_DATA
  k6-filter:
    filter-table: dt_person.dvh_person_ident_off_id
    filter-col: off_id
    timestamp: kafka_timestamp
    col: personIdent
transform:
  - src: kafka_message
    dst: melding
  - src: kafka_topic
    dst: kafka_topic
  - src: kafka_offset
    dst: kafka_offset
  - src: kafka_timestamp
    dst: kafka_mottatt_dato
    fun: int-unix-ms -> datetime-no
  - src: kafka_partition
    dst: kafka_partisjon
  - src: kafka_hash
    dst: kafka_hash
  - src: $$$BATCH_TIME
    dst: lastet_dato
  - src: $$$BATCH_TIME
    dst: oppdatert_dato
"""


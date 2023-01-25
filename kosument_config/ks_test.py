config="""
source:
  type: kafka
  batch-size: 50
  batch-interval: 5
  topic: teamfamilie.aapen-kontantstotte-vedtak-v1
  schema: json
  keypath-seperator: /
target:
  type: oracle
  skip-duplicates-with: 
    - kafka_offset
  table: dvh_fam_ks.fam_ks_meta_data_test
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
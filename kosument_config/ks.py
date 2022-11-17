config="""
source:
  type: kafka
  batch-size: 50
  batch-interval: 5
  topic: teamfamilie.aapen-ensligforsorger-vedtak-v1
  schema: json
target:
  type: oracle
  skip-duplicates-with: kafka_offset
  table: DVH_FAM_EF.KAFKA_NY_LØSNING_TEST
transform:
  - src: kafka_message
    dst: kafka_message
  - src: kafka_key
    dst: kafka_key
  - src: kafka_topic
    dst: kafka_topic
  - src: kafka_offset
    dst: kafka_offset
  - src: kafka_timestamp
    dst: kafka_timestamp
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

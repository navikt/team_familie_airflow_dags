config="""
source:
  type: kafka
  batch-size: 50
  batch-interval: 5
  topic: k9saksbehandling.aapen-k9-stonadstatistikk-v1
  group-id: dvh_familie_konsument
  schema: json
  keypath-seperator: /
target:
  type: oracle
  custom-config:
    - method: oracledb.Cursor.setinputsizes
      name: melding
      value: oracledb.DB_TYPE_CLOB
  skip-duplicates-with: 
    - kafka_offset
    - kafka_topic
  table: dvh_fam_pp.fam_pp_meta_data
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
    dst: kafka_partition
  - src: $$$BATCH_TIME
    dst: lastet_dato
"""
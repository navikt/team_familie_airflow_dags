config="""
source:
  type: kafka
  batch-size: 500
  batch-interval: 5
  topic: {}
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
  table: DVH_FAM_BB.FAM_BB_META_DATA_ORD
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
  - src: stønadstype
    dst: stonadstype
"""
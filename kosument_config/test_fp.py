config="""
source:
  type: kafka
  batch-size: 5000
  batch-interval: 5
  topic: {}
  group-id: dvh_familie_konsument_test
  schema: json
target:
  type: oracle
  custom-config:
    - method: oracledb.Cursor.setinputsizes
      name: melding
      value: oracledb.DB_TYPE_BLOB
  table: DVH_FAM_FP.TEST_FAM_FP_META_DATA
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
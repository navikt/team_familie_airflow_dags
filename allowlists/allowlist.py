slack_allowlist = ['slack.com','hooks.slack.com']

prod_oracle_conn_id = ['dm09-scan.adeo.no:1521']
dev_oracle_conn_id = ['dm07-scan.adeo.no:1521']

prod_aiven_conn_ip = ['nav-prod-kafka-nav-prod.aivencloud.com:26484', 'nav-prod-kafka-nav-prod.aivencloud.com:26487']
dev_aiven_conn_ip = ['nav-dev-kafka-nav-dev.aivencloud.com:26484', 'nav-dev-kafka-nav-dev.aivencloud.com:26487']


prod_kafka = prod_oracle_conn_id + prod_aiven_conn_ip
dev_kafka = dev_oracle_conn_id + dev_aiven_conn_ip + slack_allowlist

prod_oracle_slack = prod_oracle_conn_id + slack_allowlist
dev_oracle_slack = dev_oracle_conn_id + slack_allowlist
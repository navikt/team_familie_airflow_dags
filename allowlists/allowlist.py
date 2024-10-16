slack_allowlist = ['slack.com','hooks.slack.com']

prod_oracle_conn_id = ['dm08-scan.adeo.no:1521']
r_oracle_conn_id = ['dm07-scan.adeo.no:1521', 'hub.getdbt.com'],
dev_oracle_conn_id = ['dmv07-scan.adeo.no:1521']

prod_aiven_conn_ip = ['nav-prod-kafka-nav-prod.aivencloud.com:26484', 'nav-prod-kafka-nav-prod.aivencloud.com:26487']
r_aiven_conn_ip = ['nav-prod-kafka-nav-prod.aivencloud.com:26484', 'nav-prod-kafka-nav-prod.aivencloud.com:26487']
dev_aiven_conn_ip = ['nav-dev-kafka-nav-dev.aivencloud.com:26484', 'nav-dev-kafka-nav-dev.aivencloud.com:26487']


prod_kafka = prod_oracle_conn_id + prod_aiven_conn_ip
r_kafka = r_oracle_conn_id + r_aiven_conn_ip
dev_kafka = dev_oracle_conn_id + dev_aiven_conn_ip

prod_oracle_slack = prod_oracle_conn_id + slack_allowlist
r_oracle_slack = r_oracle_conn_id + slack_allowlist
dev_oracle_slack = dev_oracle_conn_id + slack_allowlist
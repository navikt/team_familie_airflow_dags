print("Hello World")
print("Hello World")

from kafka import KafkaConsumer

consumer = KafkaConsumer(
        topic='bidrag.statistikk-bidrag-q2',
        bootstrap_servers=['nav-dev-kafka-nav-dev.aivencloud.com:26484'],
        group_id='dvh_familie_konsument',
        # Add other necessary configurations (e.g., security, deserializers)
    )
consumer = KafkaConsumer(bootstrap_servers='nav-dev-kafka-nav-dev.aivencloud.com:26484', auto_offset_reset='earliest')
consumer.subscribe(['bidrag.statistikk-bidrag-q2'])

for message in consumer:
    headers = message.bb
    for header in headers:
        print(f"Header Key: {header.key}, Header Value: {header.value}")
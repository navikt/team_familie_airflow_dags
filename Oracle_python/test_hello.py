print("Hello World")
print("Hello World")

from kafka import KafkaConsumer

consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['nav-dev-kafka-139'],
        group_id='dvh_familie_konsument',
        # Add other necessary configurations (e.g., security, deserializers)
    )

for message in consumer:
    headers = message.bb
    for header in headers:
        print(f"Header Key: {header.key}, Header Value: {header.value}")
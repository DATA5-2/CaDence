from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
    'testing12',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    key_deserializer=lambda x: x.decode('utf-8') 
)

print("Listening for messages on topic 'testing'...")


for message in consumer:
    try:
        record = message.value
        session_id = message.key
        print(f"Received message from sessionId={session_id}: {record}")
    except Exception as e:
        print(f"Error processing message: {message}. Error: {e}")

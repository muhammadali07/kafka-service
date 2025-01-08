from kafka import KafkaConsumer
import json

def create_consumer(topic):
    return KafkaConsumer(topic, bootstrap_servers='localhost:9092', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

def consume_messages(consumer):
    for message in consumer:
        print(f"received messages: {message.value}")

if __name__ == '__main__':
    topic = 'test'
    consumer = create_consumer(topic)
    consume_messages(consumer)
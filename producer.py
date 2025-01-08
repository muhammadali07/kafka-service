from kafka import KafkaProducer
import json
import time

def create_producer():
    return KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_message(producer, topic):
    for i in range(5):
        message = {'event': f'event {i}', 'descriptino': f'description {i}'}
        producer.send(topic, message)
        time.sleep(1) # sleep for 1 second (jeda waktu 1 detik)


if __name__ == '__main__':
    topic = 'test'
    producer = create_producer()
    send_message(producer, topic)


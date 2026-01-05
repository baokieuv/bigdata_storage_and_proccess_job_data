from confluent_kafka import Producer
import json
import time
import random

conf = {
    'bootstrap.servers': 'localhost:9094',
    'client.id': 'python-producer',
    'acks': 'all',
    'retries': 5
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Error: {err}")
    else:
        print(f"Send: {msg.value().decode("utf-8")} -> Topic: {msg.topic()}")
        
topic = "demo_topic"

print("Start producer task") 

try:
    while True:
        data = {
            'event': 'transaction',
            'amount': random.randint(100, 10000),
            'status': random.choice(['SUCCESS', 'PENDING', 'FAILED']),
            'timestamp': time.time()
        }   
        
        producer.produce(
            topic,
            key=str(data['timestamp']),
            value=json.dumps(data),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1.5)
except KeyboardInterrupt:
    print("Stop produce")
finally:
    producer.flush()
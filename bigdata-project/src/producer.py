import time
import json
import datetime
import socket
import random
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = 'my-cluster-kafka-bootstrap.default.svc.cluster.local:9092'
API_URL = "http://job-api-svc/api/jobs?limit=5"
TOPIC = 'jobs-topic'

producer = None

# Retry Loop: Thử kết nối cho đến khi thành công
print(f"Producer starting on {socket.gethostname()}...")
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("Connected to Kafka successfully!")
    except NoBrokersAvailable:
        print("Kafka not ready yet, retrying in 5s...")
        time.sleep(5)
    except Exception as e:
        print(f"Unexpected error connecting to Kafka: {e}")
        time.sleep(5)

while True:
    try:
        response = requests.get(API_URL, timeout=5)
        
        if response.status_code == 200:
            jobs = response.json()
            print(f"Fetched {len(jobs)} jobs from API")
            
            for job in jobs:
                job['ingest_timestamp'] = time.time()
                producer.send(TOPIC, value=job)
            producer.flush()
            print("Batch sent to Kafka.")
        else:
            print(f"API Error: {response.status_code}")
    except Exception as e:
        print(f"Error sending data: {e}")
    time.sleep(5)
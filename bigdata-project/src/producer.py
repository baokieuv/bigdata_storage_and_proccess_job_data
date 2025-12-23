import time
import json
import datetime
import socket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

topic = 'sensor-topic'
producer = None

# Retry Loop: Thử kết nối cho đến khi thành công
print(f"Producer starting on {socket.gethostname()}...")
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['my-cluster-kafka-bootstrap.default.svc.cluster.local:9092'],
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
    data = {
        'timestamp': datetime.datetime.now().isoformat(),
        'counter': 1
    }
    try:
        producer.send(topic, value=data)
        producer.flush()
        print(f"Sent: {data}")
    except Exception as e:
        print(f"Error sending data: {e}")
    time.sleep(5)
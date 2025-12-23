import json
import time
import io
import uuid
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio

# 1. SỬA CREDENTIALS CHO KHỚP VỚI YAML MINIO
client = Minio(
    "minio.default.svc.cluster.local:9000",
    access_key="minioadmin",       # Sửa lại thành admin
    secret_key="minioadmin", # Sửa lại thành password123
    secure=False
)

consumer = None

# 2. Retry Loop cho Consumer
print("Ingestor starting...")
while consumer is None:
    try:
        consumer = KafkaConsumer(
            'sensor-topic',
            bootstrap_servers=['my-cluster-kafka-bootstrap.default.svc.cluster.local:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='minio-archiver',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Connected to Kafka successfully!")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5s...")
        time.sleep(5)

buffer = []
last_flush = time.time()

for message in consumer:
    data = message.value
    buffer.append(data)

    if (len(buffer) >= 10) or (time.time() - last_flush > 30 and len(buffer) > 0):
        file_name = f"data_{int(time.time())}_{uuid.uuid4()}.json"
        data_bytes = json.dumps(buffer).encode('utf-8')
        try:
            client.put_object(
                "sensor-data", file_name, io.BytesIO(data_bytes), len(data_bytes),
                content_type="application/json"
            )
            print(f"Uploaded {file_name} - Records: {len(buffer)}")
            buffer = []
            last_flush = time.time()
        except Exception as e:
            print(f"Upload failed: {e}")
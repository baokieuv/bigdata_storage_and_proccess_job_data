import json
import time
import io
import uuid
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio

# --- Configuration ---
KAFKA_BOOTSTRAP = ['my-cluster-kafka-bootstrap.default.svc.cluster.local:9092']
MINIO_ENDPOINT = "minio.default.svc.cluster.local:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "job-raw-data"
TOPIC = 'jobs-topic'

# --- MinIO Setup ---
client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

# --- Kafka Consumer Setup ---
consumer = None
print("MinIO Archiver starting...")
while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='minio-job-archiver-v2',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Connected to Kafka successfully!")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5s...")
        time.sleep(5)

BATCH_SIZE = 50         # Đủ 50 bản ghi thì ghi file
BATCH_TIMEOUT = 60      # Hoặc đủ 60 giây (1 phút) thì ghi file

buffer = []
last_flush = time.time()

print(f"Listening... (Flush rule: {BATCH_SIZE} records or {BATCH_TIMEOUT}s)")

for message in consumer:
    data = message.value
    if 'ingest_timestamp' not in data:
        data['ingest_timestamp'] = time.time()
        
    buffer.append(data)

    current_time = time.time()
    if (len(buffer) >= BATCH_SIZE) or ((current_time - last_flush > BATCH_TIMEOUT) and len(buffer) > 0):
        date_str = datetime.now().strftime("%Y-%m-%d")
        file_name = f"raw/event_date={date_str}/job_batch_{int(current_time)}_{uuid.uuid4()}.json"
        
        data_bytes = json.dumps(buffer).encode('utf-8')
        try:
            client.put_object(
                BUCKET_NAME, 
                file_name, 
                io.BytesIO(data_bytes), 
                len(data_bytes),
                content_type="application/json"
            )
            print(f"Uploaded {file_name} - Records: {len(buffer)}")
            buffer = []
            last_flush = time.time()
        except Exception as e:
            print(f"Upload failed: {e}")
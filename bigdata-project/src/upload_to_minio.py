import boto3
from botocore.client import Config
import os
from pathlib import Path

# Config
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "postings-data"
LOCAL_PATH = "D:/bigdata/data/"

# Tạo client
s3 = boto3.client('s3',
                  endpoint_url=MINIO_ENDPOINT,
                  aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY,
                  config=Config(signature_version='s3v4'))

# Tạo bucket
try:
    s3.create_bucket(Bucket=BUCKET_NAME)
    print(f"✅ Created bucket: {BUCKET_NAME}")
except Exception as e:
    print(f"ℹ️ Bucket exists or error: {e}")

# Upload 10 files
for i in range(1, 11):
    file_name = f"postings{i}.json"
    file_path = os.path.join(LOCAL_PATH, file_name)
    
    if os.path.exists(file_path):
        s3_key = f"raw/{file_name}"
        s3.upload_file(file_path, BUCKET_NAME, s3_key)
        print(f"✅ Uploaded: {file_name} → s3://{BUCKET_NAME}/{s3_key}")
    else:
        print(f"❌ File not found: {file_path}")

print("\n🎯 Upload completed! Files are in MinIO at:")
print(f"   Bucket: {BUCKET_NAME}")
print(f"   Path: raw/postings1.json to postings10.json")
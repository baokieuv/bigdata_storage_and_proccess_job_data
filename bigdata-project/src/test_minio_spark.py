"""
Test MinIO connection WITHOUT Hadoop dependencies
"""

import os

# ====== COMPLETELY DISABLE HADOOP ======
os.environ['HADOOP_HOME'] = ''
# Xóa Hadoop khỏi PATH nếu có
if 'C:\\hadoop\\bin' in os.environ['PATH']:
    os.environ['PATH'] = os.environ['PATH'].replace(';C:\\hadoop\\bin', '')

print("🧪 TEST MINIO ONLY (NO HADOOP)")

# ====== TEST 1: Using boto3 ======
print("\n1️⃣ Testing with boto3...")
try:
    import boto3
    from botocore.client import Config
    
    s3 = boto3.client('s3',
                      endpoint_url='http://localhost:9000',
                      aws_access_key_id='minioadmin',
                      aws_secret_access_key='minioadmin',
                      config=Config(signature_version='s3v4'))
    
    # List buckets
    response = s3.list_buckets()
    print("✅ MinIO connection successful!")
    print(f"📦 Buckets: {[b['Name'] for b in response['Buckets']]}")
    
    # Check job-data bucket
    try:
        # Thay đổi bucket name từ 'job-data' thành 'postings-data'
        objects = s3.list_objects_v2(Bucket='postings-data', Prefix='raw/')
        file_count = len(objects.get('Contents', []))
        print(f"📁 Files in 'job-data' bucket: {file_count}")
        
        if file_count > 0:
            print("Sample files:")
            for obj in objects.get('Contents', [])[:5]:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    except Exception as e:
        print(f"⚠️ Could not list job-data bucket: {e}")
        
except ImportError:
    print("❌ boto3 not installed. Run: pip install boto3")
except Exception as e:
    print(f"❌ Error: {e}")

# ====== TEST 2: Using direct HTTP requests ======
print("\n2️⃣ Testing with HTTP requests...")
try:
    import requests
    
    # Test MinIO health
    response = requests.get('http://localhost:9000')
    print(f"✅ MinIO API is reachable (Status: {response.status_code})")
    
except Exception as e:
    print(f"❌ HTTP test failed: {e}")

# ====== TEST 3: Using kubectl exec ======
print("\n3️⃣ Checking via kubectl...")
import subprocess
import json

try:
    # Get MinIO pod
    result = subprocess.run(
        ["kubectl", "get", "pods", "-l", "app=minio", "-o", "json"],
        capture_output=True,
        text=True
    )
    
    pods = json.loads(result.stdout)
    if pods['items']:
        pod_name = pods['items'][0]['metadata']['name']
        print(f"✅ MinIO pod: {pod_name}")
        
        # Check files using mc inside pod
        result = subprocess.run(
            ["kubectl", "exec", "-it", pod_name, "--", "mc", "ls", "local/job-data/"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            print(f"📊 Found {len(lines)} items in bucket")
            for line in lines[:5]:
                print(f"  {line}")
        else:
            print("⚠️ Could not list files in bucket")
            
except Exception as e:
    print(f"❌ kubectl check failed: {e}")

print("\n" + "=" * 50)
print("🎯 TEST COMPLETED!")
print("=" * 50)
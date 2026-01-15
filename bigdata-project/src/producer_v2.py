import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import socket

KAFKA_BROKER = 'my-cluster-kafka-bootstrap.default.svc.cluster.local:9092'
TOPIC = 'jobs-topic'

# Cấu hình API
LINKEDIN_API = "http://job-api-svc/api/jobs?limit=5"
ADZUNA_API = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
ADZUNA_APP_ID = "999bbd58"  # Thay bằng App ID của bạn
ADZUNA_APP_KEY = "1038c9cecaf3280c15e957ba4fb789c6"  # Thay bằng App Key của bạn

producer = None

print(f"Multi-source Producer starting on {socket.gethostname()}...")
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

def normalize_linkedin_job(job):
    """Chuẩn hóa dữ liệu từ LinkedIn API"""
    try:
        return {
            'job_id': job.get('job_id', ''),
            'source': 'linkedin',
            'company_name': job.get('company_name', ''),
            'title': job.get('title', ''),
            'description': job.get('description', ''),
            'location': job.get('location', ''),
            'location_country': 'US',  # LinkedIn API của bạn là US
            'location_city': job.get('location', '').split(',')[0].strip() if job.get('location') else '',
            'location_state': job.get('location', '').split(',')[1].strip() if ',' in job.get('location', '') else '',
            'salary_min': float(job.get('min_salary', 0)) if job.get('min_salary') else None,
            'salary_max': float(job.get('max_salary', 0)) if job.get('max_salary') else None,
            'salary_currency': job.get('currency', 'USD'),
            'work_type': job.get('work_type', ''),
            'formatted_work_type': job.get('formatted_work_type', ''),
            'contract_type': job.get('work_type', ''),
            'experience_level': job.get('formatted_experience_level', ''),
            'remote_allowed': bool(job.get('remote_allowed')),
            'listed_time': int(job.get('listed_time', 0)),
            'views': int(float(job.get('views', 0))) if job.get('views') else 0,
            'applies': int(float(job.get('applies', 0))) if job.get('applies') else 0,
            'ingest_timestamp': time.time(),
            'raw_data': job
        }
    except Exception as e:
        print(f"Error normalizing LinkedIn job: {e}")
        return None

def normalize_adzuna_job(job):
    """Chuẩn hóa dữ liệu từ Adzuna API"""
    try:
        location_obj = job.get('location', {})
        area = location_obj.get('area', [])
        
        return {
            'job_id': str(job.get('id', '')),
            'source': 'adzuna',
            'company_name': job.get('company', {}).get('display_name', 'Unknown'),
            'title': job.get('title', ''),
            'description': job.get('description', ''),
            'location': location_obj.get('display_name', ''),
            'location_country': area[0] if len(area) > 0 else 'UK',
            'location_city': area[-1] if len(area) > 0 else '',
            'location_state': area[-2] if len(area) > 1 else '',
            'salary_min': float(job.get('salary_min', 0)) if job.get('salary_min') else None,
            'salary_max': float(job.get('salary_max', 0)) if job.get('salary_max') else None,
            'salary_currency': 'GBP',
            'work_type': job.get('contract_time', '').upper().replace('-', '_'),
            'formatted_work_type': job.get('contract_time', '').replace('_', ' ').title(),
            'contract_type': job.get('contract_type', ''),
            'experience_level': 'Not Specified',
            'remote_allowed': False,
            'listed_time': int(time.mktime(time.strptime(job.get('created', ''), '%Y-%m-%dT%H:%M:%SZ')) * 1000) if job.get('created') else 0,
            'views': 0,
            'applies': 0,
            'category': job.get('category', {}).get('label', ''),
            'latitude': job.get('latitude'),
            'longitude': job.get('longitude'),
            'ingest_timestamp': time.time(),
            'raw_data': job
        }
    except Exception as e:
        print(f"Error normalizing Adzuna job: {e}")
        return None

def fetch_linkedin_jobs():
    """Lấy dữ liệu từ LinkedIn API"""
    try:
        response = requests.get(LINKEDIN_API, timeout=5)
        if response.status_code == 200:
            jobs = response.json()
            print(f"Fetched {len(jobs)} jobs from LinkedIn API")
            normalized_jobs = []
            for job in jobs:
                normalized = normalize_linkedin_job(job)
                if normalized:
                    normalized_jobs.append(normalized)
            return normalized_jobs
        else:
            print(f"LinkedIn API Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching LinkedIn jobs: {e}")
        return []

def fetch_adzuna_jobs():
    """Lấy dữ liệu từ Adzuna API"""
    try:
        params = {
            'app_id': ADZUNA_APP_ID,
            'app_key': ADZUNA_APP_KEY,
            'results_per_page': 10,
            # 'what': 'developer',  # Từ khóa tìm kiếm
            'content-type': 'application/json'
        }
        response = requests.get(ADZUNA_API, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            jobs = data.get('results', [])
            print(f"Fetched {len(jobs)} jobs from Adzuna API")
            normalized_jobs = []
            for job in jobs:
                normalized = normalize_adzuna_job(job)
                if normalized:
                    normalized_jobs.append(normalized)
            return normalized_jobs
        else:
            print(f"Adzuna API Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching Adzuna jobs: {e}")
        return []

# Main loop
iteration = 0
while True:
    try:
        all_jobs = []
        
        # Fetch từ cả 2 nguồn
        linkedin_jobs = fetch_linkedin_jobs()
        all_jobs.extend(linkedin_jobs)
        
        # Chỉ fetch Adzuna mỗi 2 lần để tránh rate limit
        if iteration % 5 == 0:
            adzuna_jobs = fetch_adzuna_jobs()
            all_jobs.extend(adzuna_jobs)
        
        # Gửi tất cả jobs vào Kafka
        if all_jobs:
            for job in all_jobs:
                producer.send(TOPIC, value=job)
            producer.flush()
            print(f"Sent {len(all_jobs)} normalized jobs to Kafka")
        else:
            print("No jobs to send")
            
        iteration += 1
        time.sleep(30)  # Tăng interval để tránh rate limit
        
    except Exception as e:
        print(f"Error in main loop: {e}")
        time.sleep(5)
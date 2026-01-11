import json
import random
from datetime import datetime, timedelta
import os

# Dữ liệu mẫu để random
companies = [
    "Corcoran Sawyer Smith", "Tech Innovations Inc", "Global Solutions Ltd", 
    "Acme Corporation", "Future Systems", "Digital Ventures", "Prime Industries",
    "Alpha Technologies", "Beta Consulting", "Gamma Services"
]

titles = [
    "Marketing Coordinator", "Software Engineer", "Data Analyst", "Product Manager",
    "Sales Representative", "Project Manager", "Business Analyst", "DevOps Engineer",
    "UX Designer", "Financial Analyst", "HR Manager", "Customer Success Manager"
]

experience_levels = [
    "Entry level", "Associate", "Mid-Senior level", "Director", "Executive", ""
]

work_types = [
    "Full-time", "Part-time", "Contract", "Temporary", "Internship", ""
]

locations = [
    "Princeton, NJ", "New York, NY", "San Francisco, CA", "Boston, MA",
    "Seattle, WA", "Austin, TX", "Chicago, IL", "Denver, CO"
]

def generate_job_posting(job_id):
    """Tạo một job posting ngẫu nhiên"""
    experience = random.choice(experience_levels)
    work_type = random.choice(work_types)
    
    # Random salary dựa trên experience level
    if experience == "Entry level":
        salary = random.randint(40000, 60000)
    elif experience == "Associate":
        salary = random.randint(55000, 80000)
    elif experience == "Mid-Senior level":
        salary = random.randint(75000, 120000)
    elif experience == "Director":
        salary = random.randint(110000, 180000)
    elif experience == "Executive":
        salary = random.randint(150000, 300000)
    else:
        salary = random.randint(40000, 120000)
    
    return {
        "job_id": str(job_id),
        "company_name": random.choice(companies),
        "title": random.choice(titles),
        "description": f"Job description for {random.choice(titles)} position. We are looking for a talented professional...",
        "max_salary": str(float(salary * 1.1)),
        "pay_period": random.choice(["YEARLY", "HOURLY", "MONTHLY"]),
        "location": random.choice(locations),
        "company_id": str(random.randint(1000000, 9999999)),
        "views": str(random.randint(10, 500)),
        "med_salary": str(float(salary)),
        "min_salary": str(float(salary * 0.9)),
        "formatted_work_type": work_type,
        "applies": str(random.randint(0, 50)),
        "original_listed_time": str(int(datetime.now().timestamp() * 1000)),
        "remote_allowed": str(random.randint(0, 1)),
        "job_posting_url": f"https://www.linkedin.com/jobs/view/{job_id}/",
        "application_url": "",
        "application_type": random.choice(["ComplexOnsiteApply", "SimpleOnsiteApply", "OffsiteApply"]),
        "expiry": str(int((datetime.now() + timedelta(days=30)).timestamp() * 1000)),
        "closed_time": "",
        "formatted_experience_level": experience,
        "skills_desc": "Requirements: Strong communication skills, team player, detail-oriented...",
        "listed_time": str(int(datetime.now().timestamp() * 1000)),
        "posting_domain": "linkedin.com",
        "sponsored": str(random.randint(0, 1)),
        "work_type": work_type.upper().replace("-", "_") if work_type else "",
        "currency": "USD",
        "compensation_type": "BASE_SALARY",
        "normalized_salary": str(float(salary)),
        "zip_code": str(random.randint(10000, 99999)),
        "fips": str(random.randint(1000, 9999))
    }

def generate_daily_files(num_days=20, jobs_per_day_range=(1, 3)):
    """Tạo 20 file JSON, mỗi file một ngày"""
    start_date = datetime(2026, 1, 1)
    output_dir = "sample_data"
    
    # Tạo thư mục output nếu chưa có
    os.makedirs(output_dir, exist_ok=True)
    
    job_id_counter = 900000
    
    for day in range(num_days):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")
        filename = f"jobs_{date_str}.json"
        filepath = os.path.join(output_dir, filename)
        
        # Random số lượng job trong ngày
        num_jobs = random.randint(*jobs_per_day_range)
        
        # Tạo list các job postings
        jobs = []
        for _ in range(num_jobs):
            job_id_counter += 1
            jobs.append(generate_job_posting(job_id_counter))
        
        # Ghi ra file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Đã tạo {filename} với {num_jobs} job postings")
    
    print(f"\n✓ Hoàn thành! Đã tạo {num_days} file trong thư mục '{output_dir}'")

if __name__ == "__main__":
    generate_daily_files()

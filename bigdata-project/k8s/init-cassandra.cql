-- Tạo keyspace
CREATE KEYSPACE IF NOT EXISTS metrics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE metrics;

-- Bảng 1: Lưu lương trung bình theo Experience Level
CREATE TABLE IF NOT EXISTS avg_salary_by_experience (
    id UUID PRIMARY KEY,
    experience_level TEXT,
    avg_salary DOUBLE,
    job_count INT,
    process_date DATE,
    processed_at TIMESTAMP
);

-- Index để query theo experience_level và process_date
CREATE INDEX IF NOT EXISTS idx_experience_level ON avg_salary_by_experience(experience_level);
CREATE INDEX IF NOT EXISTS idx_process_date_exp ON avg_salary_by_experience(process_date);

-- Bảng 2: Lưu số lượng job theo Work Type
CREATE TABLE IF NOT EXISTS jobs_by_work_type (
    id UUID PRIMARY KEY,
    work_type TEXT,
    job_count INT,
    process_date DATE,
    processed_at TIMESTAMP
);

-- Index để query theo work_type và process_date
CREATE INDEX IF NOT EXISTS idx_work_type ON jobs_by_work_type(work_type);
CREATE INDEX IF NOT EXISTS idx_process_date_work ON jobs_by_work_type(process_date);

-- Bảng 3: Audit log để tracking job processing
CREATE TABLE IF NOT EXISTS job_processing_audit (
    id UUID PRIMARY KEY,
    process_date DATE,
    job_name TEXT,
    status TEXT,  -- SUCCESS, FAILED, RUNNING
    records_read INT,
    records_written_exp INT,
    records_written_work INT,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Index để query audit logs
CREATE INDEX IF NOT EXISTS idx_audit_date ON job_processing_audit(process_date);
CREATE INDEX IF NOT EXISTS idx_audit_status ON job_processing_audit(status);
CREATE INDEX IF NOT EXISTS idx_audit_started ON job_processing_audit(started_at);

# Script khởi tạo Cassandra schema
# Chạy PowerShell

Write-Host "=== Initializing Cassandra Schema ===" -ForegroundColor Green

$cqlFile = Join-Path $PSScriptRoot "..\..\k8s\init-cassandra.cql"

if (!(Test-Path $cqlFile)) {
    Write-Host "Error: init-cassandra.cql not found at $cqlFile" -ForegroundColor Red
    exit 1
}

# Execute CQL commands directly
Write-Host "Creating Cassandra schema..." -ForegroundColor Yellow

# Create keyspace
Write-Host "  Creating keyspace..." -ForegroundColor Yellow
kubectl exec cassandra-0 -- cqlsh -e "CREATE KEYSPACE IF NOT EXISTS metrics WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"

# Create table 1
Write-Host "  Creating avg_salary_by_experience table..." -ForegroundColor Yellow
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE TABLE IF NOT EXISTS avg_salary_by_experience (id UUID PRIMARY KEY, experience_level TEXT, avg_salary DOUBLE, job_count INT, process_date DATE, processed_at TIMESTAMP);"

# Create indexes for table 1
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE INDEX IF NOT EXISTS idx_experience_level ON avg_salary_by_experience(experience_level);"
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE INDEX IF NOT EXISTS idx_process_date_exp ON avg_salary_by_experience(process_date);"

# Create table 2
Write-Host "  Creating jobs_by_work_type table..." -ForegroundColor Yellow
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE TABLE IF NOT EXISTS jobs_by_work_type (id UUID PRIMARY KEY, work_type TEXT, job_count INT, process_date DATE, processed_at TIMESTAMP);"

# Create indexes for table 2
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE INDEX IF NOT EXISTS idx_work_type ON jobs_by_work_type(work_type);"
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE INDEX IF NOT EXISTS idx_process_date_work ON jobs_by_work_type(process_date);"

# Create table 3
Write-Host "  Creating job_processing_audit table..." -ForegroundColor Yellow
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE TABLE IF NOT EXISTS job_processing_audit (id UUID PRIMARY KEY, process_date DATE, job_name TEXT, status TEXT, records_read INT, records_written_exp INT, records_written_work INT, error_message TEXT, started_at TIMESTAMP, completed_at TIMESTAMP);"

# Create indexes for table 3
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE INDEX IF NOT EXISTS idx_audit_date ON job_processing_audit(process_date);"
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE INDEX IF NOT EXISTS idx_audit_status ON job_processing_audit(status);"
kubectl exec cassandra-0 -- cqlsh -e "USE metrics; CREATE INDEX IF NOT EXISTS idx_audit_started ON job_processing_audit(started_at);"

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Schema initialized successfully" -ForegroundColor Green
} else {
    Write-Host "Error: Failed to initialize schema" -ForegroundColor Red
    exit 1
}

# Verify
Write-Host ""
Write-Host "Verifying schema..." -ForegroundColor Yellow
kubectl exec -it cassandra-0 -- cqlsh -e "DESCRIBE KEYSPACE metrics;"

Write-Host ""
Write-Host "=== Cassandra Initialization Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Next step:" -ForegroundColor Cyan
Write-Host "Run: .\7-build-spark-image.ps1" -ForegroundColor Yellow

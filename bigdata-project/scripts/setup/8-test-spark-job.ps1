# Script test Spark job với một ngày dữ liệu
# Chạy PowerShell

Write-Host "=== Testing Spark Job ===" -ForegroundColor Green

$k8sPath = Join-Path $PSScriptRoot "..\..\k8s"
$templateFile = Join-Path $k8sPath "spark-job-template.yaml"
$testFile = Join-Path $k8sPath "spark-job-test.yaml"
$testDate = "2026-01-01"

# Tạo test job file
Write-Host "Creating test job for date: $testDate" -ForegroundColor Yellow
(Get-Content $templateFile) -replace 'DATEPLACEHOLDER', $testDate | Set-Content $testFile

# Apply job
Write-Host "Submitting Spark job..." -ForegroundColor Yellow
kubectl apply -f $testFile

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to submit job" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Job submitted" -ForegroundColor Green

# Wait for job to start
Write-Host ""
Write-Host "Waiting for job pod to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Get pod name
$podName = kubectl get pods -l job-name=spark-job-$testDate -o jsonpath='{.items[0].metadata.name}' 2>$null

if ($podName) {
    Write-Host "[OK] Job pod started: $podName" -ForegroundColor Green
    Write-Host ""
    Write-Host "Streaming logs (Ctrl+C to stop):" -ForegroundColor Cyan
    Write-Host "================================================" -ForegroundColor Cyan
    
    kubectl logs -f $podName
    
    Write-Host ""
    Write-Host "================================================" -ForegroundColor Cyan
} else {
    Write-Host "Waiting for pod to be created..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
    $podName = kubectl get pods -l job-name=spark-job-$testDate -o jsonpath='{.items[0].metadata.name}'
    
    if ($podName) {
        Write-Host "[OK] Job pod started: $podName" -ForegroundColor Green
        kubectl logs -f $podName
    } else {
        Write-Host "Warning: Could not find job pod" -ForegroundColor Yellow
        Write-Host "Check manually with: kubectl get pods" -ForegroundColor Yellow
    }
}

# Check job status
Write-Host ""
Write-Host "Checking job status..." -ForegroundColor Yellow
$jobStatus = kubectl get job spark-job-$testDate -o jsonpath='{.status.succeeded}'

if ($jobStatus -eq "1") {
    Write-Host "[OK] Job completed successfully!" -ForegroundColor Green
    
    # Verify data in Cassandra
    Write-Host ""
    Write-Host "Verifying data in Cassandra..." -ForegroundColor Yellow
    
    Write-Host ""
    Write-Host "Average salary by experience level:" -ForegroundColor Cyan
    kubectl exec -it cassandra-0 -- cqlsh -e "SELECT experience_level, avg_salary, job_count FROM metrics.avg_salary_by_experience WHERE process_date='$testDate' ALLOW FILTERING;"
    
    Write-Host ""
    Write-Host "Jobs by work type:" -ForegroundColor Cyan
    kubectl exec -it cassandra-0 -- cqlsh -e "SELECT work_type, job_count FROM metrics.jobs_by_work_type WHERE process_date='$testDate' ALLOW FILTERING;"
    
    Write-Host ""
    Write-Host "Audit log:" -ForegroundColor Cyan
    kubectl exec -it cassandra-0 -- cqlsh -e "SELECT process_date, status, records_read, records_written_exp, records_written_work FROM metrics.job_processing_audit WHERE process_date='$testDate' ALLOW FILTERING;"
    
    Write-Host ""
    Write-Host "=== Spark Job Test Complete ===" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next step (optional):" -ForegroundColor Cyan
    Write-Host "Run: .\9-deploy-airflow.ps1 (for automation)" -ForegroundColor Yellow
    
} else {
    Write-Host "Job may still be running or failed" -ForegroundColor Yellow
    Write-Host "Check status with: kubectl get jobs" -ForegroundColor Yellow
    Write-Host "View logs with: kubectl logs $podName" -ForegroundColor Yellow
}

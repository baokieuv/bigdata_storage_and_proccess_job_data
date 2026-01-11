# Master script - Chạy tất cả setup tự động
# Chạy PowerShell as Administrator

param(
    [switch]$SkipPrerequisites = $false
)

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot

Write-Host "=============================================================" -ForegroundColor Cyan
Write-Host "                                                             " -ForegroundColor Cyan
Write-Host "      BigData Pipeline - Automated Setup Script             " -ForegroundColor Cyan
Write-Host "                                                             " -ForegroundColor Cyan
Write-Host "=============================================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Prerequisites
if (!$SkipPrerequisites) {
    Write-Host "[1/9] Installing Prerequisites..." -ForegroundColor Magenta
    & "$scriptDir\1-install-prerequisites.ps1"
    if ($LASTEXITCODE -ne 0) { exit 1 }
    Write-Host ""
    Read-Host "Press Enter to continue..."
} else {
    Write-Host "[1/9] Skipping Prerequisites (-SkipPrerequisites flag)" -ForegroundColor Yellow
}

# Step 2: Start Minikube
Write-Host ""
Write-Host "[2/9] Starting Minikube..." -ForegroundColor Magenta
& "$scriptDir\2-start-minikube.ps1"
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host ""
Read-Host "Press Enter to continue..."

# Step 3: Generate Data
Write-Host ""
Write-Host "[3/9] Generating Sample Data..." -ForegroundColor Magenta
& "$scriptDir\3-generate-data.ps1"
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host ""
Read-Host "Press Enter to continue..."

# Step 4: Deploy Infrastructure
Write-Host ""
Write-Host "[4/9] Deploying Infrastructure (MinIO, Cassandra, Spark)..." -ForegroundColor Magenta
& "$scriptDir\4-deploy-infrastructure.ps1"
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host ""
Read-Host "Press Enter to continue..."

# Step 5: Setup MinIO
Write-Host ""
Write-Host "[5/9] Setting up MinIO and Uploading Data..." -ForegroundColor Magenta
& "$scriptDir\5-setup-minio.ps1"
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host ""
Read-Host "Press Enter to continue..."

# Step 6: Init Cassandra
Write-Host ""
Write-Host "[6/9] Initializing Cassandra Schema..." -ForegroundColor Magenta
& "$scriptDir\6-init-cassandra.ps1"
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host ""
Read-Host "Press Enter to continue..."

# Step 7: Build Spark Image
Write-Host ""
Write-Host "[7/9] Building Spark Job Image..." -ForegroundColor Magenta
& "$scriptDir\7-build-spark-image.ps1"
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host ""
Read-Host "Press Enter to continue..."

# Step 8: Test Spark Job
Write-Host ""
Write-Host "[8/9] Testing Spark Job..." -ForegroundColor Magenta
& "$scriptDir\8-test-spark-job.ps1"
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host ""

# Step 9: Deploy Airflow (Optional)
Write-Host ""
Write-Host "[9/9] Deploy Airflow for Automation?" -ForegroundColor Magenta
Write-Host "This is optional. Skip if you only want to test Spark manually." -ForegroundColor Yellow
$deployAirflow = Read-Host "Deploy Airflow? (y/n)"

if ($deployAirflow -eq "y") {
    Write-Host ""
    & "$scriptDir\9-deploy-airflow.ps1"
    if ($LASTEXITCODE -ne 0) { exit 1 }
} else {
    Write-Host "Skipping Airflow deployment" -ForegroundColor Yellow
}

# Done
Write-Host ""
Write-Host "=============================================================" -ForegroundColor Green
Write-Host "                                                             " -ForegroundColor Green
Write-Host "              Setup Complete!                                " -ForegroundColor Green
Write-Host "                                                             " -ForegroundColor Green
Write-Host "=============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Your BigData pipeline is now running!" -ForegroundColor Cyan
Write-Host ""
Write-Host "Access points:" -ForegroundColor Cyan
Write-Host "  • MinIO Console: http://localhost:9001 (admin/minioadmin)" -ForegroundColor Yellow
Write-Host "  • Spark UI: http://localhost:8080" -ForegroundColor Yellow
if ($deployAirflow -eq "y") {
    Write-Host "  • Airflow UI: http://localhost:8081 (admin/admin)" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Cyan
Write-Host "  • Check pods: kubectl get pods -A" -ForegroundColor Yellow
Write-Host "  • Query Cassandra: kubectl exec -it cassandra-0 -- cqlsh" -ForegroundColor Yellow
Write-Host "  • View jobs: kubectl get jobs" -ForegroundColor Yellow
Write-Host ""
Write-Host "To run Spark job manually for another date:" -ForegroundColor Cyan
Write-Host "  cd bigdata-project\scripts\setup" -ForegroundColor Yellow
Write-Host "  .\8-test-spark-job.ps1" -ForegroundColor Yellow

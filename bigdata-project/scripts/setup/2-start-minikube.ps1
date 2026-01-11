# Script khởi động Minikube
# Chạy PowerShell

Write-Host "=== Starting Minikube ===" -ForegroundColor Green

# Check Docker running
$dockerRunning = docker ps 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Docker Desktop is not running!" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and wait until it's ready, then run this script again." -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Docker Desktop is running" -ForegroundColor Green

# Start Minikube với memory thấp hơn để phù hợp với hầu hết máy
Write-Host "Starting Minikube cluster (this may take 3-5 minutes)..." -ForegroundColor Yellow
Write-Host "Using 6GB memory (adjust Docker Desktop settings if you have more RAM)" -ForegroundColor Cyan
minikube start --driver=docker --cpus=4 --memory=6144 --disk-size=30g

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to start Minikube" -ForegroundColor Red
    Write-Host "" -ForegroundColor Yellow
    Write-Host "If memory error, try one of these:" -ForegroundColor Yellow
    Write-Host "1. Increase Docker Desktop memory: Settings -> Resources -> Memory" -ForegroundColor Yellow
    Write-Host "2. Or run with less memory: minikube start --driver=docker --cpus=2 --memory=4096" -ForegroundColor Yellow
    Write-Host "3. Or delete and retry: minikube delete && minikube start --driver=docker --cpus=4 --memory=6144" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Minikube started successfully" -ForegroundColor Green

# Verify
Write-Host ""
Write-Host "Verifying cluster..." -ForegroundColor Yellow
kubectl get nodes

Write-Host ""
Write-Host "=== Minikube Ready ===" -ForegroundColor Green
Write-Host ""
Write-Host "Next step:" -ForegroundColor Cyan
Write-Host "Run: .\3-generate-data.ps1" -ForegroundColor Yellow

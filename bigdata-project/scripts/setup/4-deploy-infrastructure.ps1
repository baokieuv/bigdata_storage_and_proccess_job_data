# Script deploy tất cả infrastructure
# Chạy PowerShell

Write-Host "=== Deploying Infrastructure ===" -ForegroundColor Green

$k8sPath = Join-Path $PSScriptRoot "..\..\k8s"

# Deploy MinIO
Write-Host ""
Write-Host "1. Deploying MinIO..." -ForegroundColor Yellow
kubectl apply -f (Join-Path $k8sPath "minio-config.yaml")
Write-Host "   Waiting for MinIO pods to be ready (30s)..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=minio --timeout=300s
Write-Host "   [OK] MinIO deployed" -ForegroundColor Green

# Deploy Cassandra
Write-Host ""
Write-Host "2. Deploying Cassandra..." -ForegroundColor Yellow
kubectl apply -f (Join-Path $k8sPath "cassandra-config.yaml")
Write-Host "   Waiting for Cassandra pods to be ready (may take 2-3 minutes)..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=cassandra --timeout=600s
Write-Host "   [OK] Cassandra deployed" -ForegroundColor Green

# Deploy Spark
Write-Host ""
Write-Host "3. Deploying Spark..." -ForegroundColor Yellow
kubectl apply -f (Join-Path $k8sPath "spark-config.yaml")
Write-Host "   Waiting for Spark Master to be ready..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=spark-master --timeout=300s
Write-Host "   Waiting for Spark Workers to be ready..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=spark-worker --timeout=300s
Write-Host "   [OK] Spark deployed" -ForegroundColor Green

Write-Host ""
Write-Host "=== Infrastructure Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "All pods status:" -ForegroundColor Cyan
kubectl get pods

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Run: .\5-setup-minio.ps1 (upload data)" -ForegroundColor Yellow
Write-Host "2. Run: .\6-init-cassandra.ps1 (create schema)" -ForegroundColor Yellow

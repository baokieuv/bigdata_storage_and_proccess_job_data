# Script build Spark image và load vào Minikube
# Chạy PowerShell

Write-Host "=== Building Spark Job Image ===" -ForegroundColor Green

$projectRoot = Join-Path $PSScriptRoot "..\..\"

# Hỏi user có muốn push lên Docker Hub không
Write-Host ""
Write-Host "Do you want to push image to Docker Hub? (y/n)" -ForegroundColor Yellow
Write-Host "If 'n', image will be loaded directly into Minikube (faster, no registry needed)" -ForegroundColor Cyan
$pushChoice = Read-Host "Choice"

if ($pushChoice -eq "y") {
    # Push to Docker Hub
    Write-Host ""
    $username = Read-Host "Enter your Docker Hub username"
    $imageName = "$username/spark-job:latest"
    
    Write-Host ""
    Write-Host "Logging in to Docker Hub..." -ForegroundColor Yellow
    docker login
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Docker login failed" -ForegroundColor Red
        exit 1
    }
    
    Write-Host ""
    Write-Host "Building image: $imageName" -ForegroundColor Yellow
    docker build -f (Join-Path $projectRoot "Dockerfile.spark") -t $imageName $projectRoot
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Docker build failed" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "[OK] Image built" -ForegroundColor Green
    
    Write-Host ""
    Write-Host "Pushing image to Docker Hub..." -ForegroundColor Yellow
    docker push $imageName
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Docker push failed" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "[OK] Image pushed" -ForegroundColor Green
    
    # Update config files
    Write-Host ""
    Write-Host "Updating configuration files with image name..." -ForegroundColor Yellow
    
    $sparkJobTemplate = Join-Path $projectRoot "k8s\spark-job-template.yaml"
    $dagFile = Join-Path $projectRoot "dags\job_processing_dag.py"
    
    (Get-Content $sparkJobTemplate) -replace 'your-docker-registry/spark-job:latest', $imageName | Set-Content $sparkJobTemplate
    (Get-Content $dagFile) -replace 'your-docker-registry/spark-job:latest', $imageName | Set-Content $dagFile
    
    Write-Host "[OK] Configuration updated" -ForegroundColor Green
    Write-Host "   Image name: $imageName" -ForegroundColor Cyan
    
} else {
    # Load into Minikube
    $imageName = "spark-job:latest"
    
    Write-Host ""
    Write-Host "Building image: $imageName" -ForegroundColor Yellow
    
    # Build trong Minikube environment
    & minikube -p minikube docker-env --shell powershell | Invoke-Expression
    docker build -f (Join-Path $projectRoot "Dockerfile.spark") -t $imageName $projectRoot
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Docker build failed" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "[OK] Image built and loaded into Minikube" -ForegroundColor Green
    
    # Update config files
    Write-Host ""
    Write-Host "Updating configuration files with image name..." -ForegroundColor Yellow
    
    $sparkJobTemplate = Join-Path $projectRoot "k8s\spark-job-template.yaml"
    $dagFile = Join-Path $projectRoot "dags\job_processing_dag.py"
    
    (Get-Content $sparkJobTemplate) -replace 'your-docker-registry/spark-job:latest', $imageName | Set-Content $sparkJobTemplate
    (Get-Content $sparkJobTemplate) -replace 'imagePullPolicy: Always', 'imagePullPolicy: Never' | Set-Content $sparkJobTemplate
    
    (Get-Content $dagFile) -replace 'your-docker-registry/spark-job:latest', $imageName | Set-Content $dagFile
    
    Write-Host "[OK] Configuration updated" -ForegroundColor Green
    Write-Host "   Image name: $imageName" -ForegroundColor Cyan
    Write-Host "   Image pull policy: Never (using local image)" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "=== Spark Image Build Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Next step:" -ForegroundColor Cyan
Write-Host "Run: .\8-test-spark-job.ps1" -ForegroundColor Yellow

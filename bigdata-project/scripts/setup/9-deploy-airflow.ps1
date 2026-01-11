# Script deploy Airflow để tự động hóa
# Chạy PowerShell

Write-Host "=== Deploying Airflow ===" -ForegroundColor Green

# Add Helm repo
Write-Host "Adding Airflow Helm repository..." -ForegroundColor Yellow
helm repo add apache-airflow https://airflow.apache.org 2>&1 | Out-Null
helm repo update 2>&1 | Out-Null
Write-Host "[OK] Helm repo added" -ForegroundColor Green

# Create namespace
Write-Host "Creating Airflow namespace..." -ForegroundColor Yellow
kubectl create namespace airflow 2>&1 | Out-Null
Write-Host "[OK] Namespace created" -ForegroundColor Green

# Create values file
$valuesFile = Join-Path $PSScriptRoot "..\..\airflow-values.yaml"

$valuesContent = @"
executor: KubernetesExecutor

dags:
  gitSync:
    enabled: false

logs:
  persistence:
    enabled: true
    size: 5Gi

webserver:
  service:
    type: NodePort

env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: 'False'
  - name: AIRFLOW__KUBERNETES__IN_CLUSTER
    value: 'True'
  - name: AIRFLOW__KUBERNETES__NAMESPACE
    value: 'default'

resources:
  limits:
    cpu: 1000m
    memory: 2Gi
  requests:
    cpu: 500m
    memory: 1Gi
"@

$valuesContent | Out-File -FilePath $valuesFile -Encoding UTF8
Write-Host "[OK] Values file created" -ForegroundColor Green

# Create ConfigMap for DAGs
Write-Host "Creating ConfigMap with DAG..." -ForegroundColor Yellow
$dagFile = Join-Path $PSScriptRoot "..\..\dags\job_processing_dag.py"
kubectl create configmap airflow-dags --from-file=$dagFile -n airflow 2>&1 | Out-Null
Write-Host "[OK] DAG ConfigMap created" -ForegroundColor Green

# Install Airflow
Write-Host ""
Write-Host "Installing Airflow (this may take 5-10 minutes)..." -ForegroundColor Yellow
Write-Host "Please be patient..." -ForegroundColor Cyan

# Add volume mount for DAG
$helmValues = @"
$valuesContent

extraVolumes:
  - name: dags
    configMap:
      name: airflow-dags

extraVolumeMounts:
  - name: dags
    mountPath: /opt/airflow/dags
"@

$helmValuesFile = Join-Path $PSScriptRoot "..\..\airflow-values-full.yaml"
$helmValues | Out-File -FilePath $helmValuesFile -Encoding UTF8

helm install airflow apache-airflow/airflow -n airflow -f $helmValuesFile --timeout 15m

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install Airflow" -ForegroundColor Red
    Write-Host "Check logs with: kubectl get pods -n airflow" -ForegroundColor Yellow
    exit 1
}

# Wait for webserver
Write-Host ""
Write-Host "Waiting for Airflow webserver to be ready..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l component=webserver -n airflow --timeout=600s

Write-Host "[OK] Airflow installed successfully" -ForegroundColor Green

# Get admin password
Write-Host ""
Write-Host "Getting admin password..." -ForegroundColor Yellow
$adminPassword = kubectl get secret airflow-webserver-secret -n airflow -o jsonpath="{.data.webserver-secret-key}" | ForEach-Object { [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($_)) }

# Set initial variable
Write-Host "Setting initial Airflow variable..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

$schedulerPod = kubectl get pods -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}'
kubectl exec -n airflow $schedulerPod -- airflow variables set job_processing_current_date "2026-01-01"

Write-Host "[OK] Variable set" -ForegroundColor Green

# Port-forward (background)
Write-Host ""
Write-Host "Starting port-forward to Airflow UI..." -ForegroundColor Yellow
$portForwardJob = Start-Job -ScriptBlock {
    kubectl port-forward svc/airflow-webserver 8081:8080 -n airflow
}
Start-Sleep -Seconds 5

Write-Host ""
Write-Host "=== Airflow Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Airflow UI: http://localhost:8081" -ForegroundColor Cyan
Write-Host "Username: admin" -ForegroundColor Cyan
Write-Host "Password: admin (default)" -ForegroundColor Cyan
Write-Host ""
Write-Host "Port-forward is running in background (Job ID: $($portForwardJob.Id))" -ForegroundColor Yellow
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Open browser: http://localhost:8081" -ForegroundColor Yellow
Write-Host "2. Login with admin/admin" -ForegroundColor Yellow
Write-Host "3. Find DAG 'job_processing_pipeline' and toggle it ON" -ForegroundColor Yellow
Write-Host "4. DAG will run every 5 minutes automatically" -ForegroundColor Yellow
Write-Host ""
Write-Host "To stop port-forward: Stop-Job $($portForwardJob.Id); Remove-Job $($portForwardJob.Id)" -ForegroundColor Yellow

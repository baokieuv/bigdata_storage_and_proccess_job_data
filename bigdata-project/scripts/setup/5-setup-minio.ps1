# Script setup MinIO và upload dữ liệu
# Chạy PowerShell

Write-Host "=== Setting up MinIO ===" -ForegroundColor Green

# Install MinIO Client (mc)
if (!(Get-Command mc -ErrorAction SilentlyContinue)) {
    Write-Host "Installing MinIO Client..." -ForegroundColor Yellow
    $mcUrl = "https://dl.min.io/client/mc/release/windows-amd64/mc.exe"
    $mcPath = Join-Path $env:TEMP "mc.exe"
    Invoke-WebRequest -Uri $mcUrl -OutFile $mcPath
    Move-Item -Path $mcPath -Destination "C:\Windows\System32\mc.exe" -Force
    Write-Host "[OK] MinIO Client installed" -ForegroundColor Green
}

# Port-forward MinIO (chạy background)
Write-Host "Starting port-forward to MinIO..." -ForegroundColor Yellow
$portForwardJob = Start-Job -ScriptBlock {
    kubectl port-forward svc/minio 9000:9000 9001:9001
}
Start-Sleep -Seconds 5

# Configure mc alias
Write-Host "Configuring MinIO client..." -ForegroundColor Yellow
mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>&1 | Out-Null

# Create bucket
Write-Host "Creating bucket 'sensor-data'..." -ForegroundColor Yellow
mc mb myminio/sensor-data 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Note: Bucket may already exist (this is OK)" -ForegroundColor Yellow
}
Write-Host "[OK] Bucket created" -ForegroundColor Green

# Upload data - tìm ở nhiều vị trí có thể
$possiblePaths = @(
    (Join-Path $PSScriptRoot "..\sample_data"),
    (Join-Path $PSScriptRoot "..\..\sample_data"),
    "C:\BK_WORKSPACE\bigdata\bigdata_storage_and_proccess_job_data\bigdata-project\scripts\sample_data",
    (Join-Path (Get-Location) "sample_data")
)

$dataPath = $null
foreach ($path in $possiblePaths) {
    if (Test-Path $path) {
        $dataPath = $path
        break
    }
}

if ($dataPath) {
    Write-Host "Uploading JSON files from: $dataPath" -ForegroundColor Yellow
    
    # Get all JSON files
    $jsonFiles = Get-ChildItem -Path $dataPath -Filter "*.json"
    
    if ($jsonFiles.Count -eq 0) {
        Write-Host "Warning: No JSON files found in $dataPath" -ForegroundColor Yellow
    } else {
        # Upload each file individually (mc doesn't like wildcards in Windows)
        $uploaded = 0
        foreach ($file in $jsonFiles) {
            mc cp $file.FullName myminio/sensor-data/ 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) {
                $uploaded++
            }
        }
        Write-Host "[OK] Uploaded $uploaded files to MinIO" -ForegroundColor Green
    }
    
    # Verify
    Write-Host ""
    Write-Host "Files in bucket:" -ForegroundColor Cyan
    mc ls myminio/sensor-data/ | Select-Object -First 5
    $fileCount = (mc ls myminio/sensor-data/ | Measure-Object).Count
    Write-Host "Total files: $fileCount" -ForegroundColor Cyan
} else {
    Write-Host "Error: Sample data not found at $dataPath" -ForegroundColor Red
    Stop-Job $portForwardJob
    Remove-Job $portForwardJob
    exit 1
}

# Keep port-forward running
Write-Host ""
Write-Host "=== MinIO Setup Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "MinIO Console: http://localhost:9001" -ForegroundColor Cyan
Write-Host "Username: minioadmin" -ForegroundColor Cyan
Write-Host "Password: minioadmin" -ForegroundColor Cyan
Write-Host ""
Write-Host "Port-forward is running in background (Job ID: $($portForwardJob.Id))" -ForegroundColor Yellow
Write-Host "To stop: Stop-Job $($portForwardJob.Id); Remove-Job $($portForwardJob.Id)" -ForegroundColor Yellow
Write-Host ""
Write-Host "Next step:" -ForegroundColor Cyan
Write-Host "Run: .\6-init-cassandra.ps1" -ForegroundColor Yellow

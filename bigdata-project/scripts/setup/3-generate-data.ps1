# Script sinh dữ liệu mẫu
# Chạy PowerShell

Write-Host "=== Generating Sample Data ===" -ForegroundColor Green

$scriptPath = Join-Path $PSScriptRoot "..\generate_sample_data.py"
$outputPath = Join-Path $PSScriptRoot "..\sample_data"

if (Test-Path $scriptPath) {
    Write-Host "Running data generator..." -ForegroundColor Yellow
    python $scriptPath
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Data generated successfully" -ForegroundColor Green
        
        # Check if output path exists
        if (Test-Path $outputPath) {
            $fileCount = (Get-ChildItem -Path $outputPath -Filter "*.json").Count
            Write-Host "Generated $fileCount JSON files in: $outputPath" -ForegroundColor Cyan
        } else {
            Write-Host "Warning: Output directory not found at $outputPath" -ForegroundColor Yellow
            Write-Host "Files may have been generated in a different location" -ForegroundColor Yellow
        }
    } else {
        Write-Host "Error: Failed to generate data" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Error: generate_sample_data.py not found at $scriptPath" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Data Generation Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Next step:" -ForegroundColor Cyan
Write-Host "Run: .\4-deploy-infrastructure.ps1" -ForegroundColor Yellow

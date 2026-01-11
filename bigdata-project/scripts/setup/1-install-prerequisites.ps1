# Script tự động cài đặt prerequisites trên Windows
# Chạy PowerShell as Administrator

Write-Host "=== Installing Prerequisites ===" -ForegroundColor Green

# Kiểm tra Chocolatey
if (!(Get-Command choco -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Chocolatey..." -ForegroundColor Yellow
    Set-ExecutionPolicy Bypass -Scope Process -Force
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
    iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
    Write-Host "[OK] Chocolatey installed" -ForegroundColor Green
} else {
    Write-Host "[OK] Chocolatey already installed" -ForegroundColor Green
}

# Cài Git
if (!(Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Git..." -ForegroundColor Yellow
    choco install git -y
    Write-Host "[OK] Git installed" -ForegroundColor Green
} else {
    Write-Host "[OK] Git already installed" -ForegroundColor Green
}

# Cài Docker Desktop
if (!(Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker Desktop not found. Please install manually from:" -ForegroundColor Red
    Write-Host "https://www.docker.com/products/docker-desktop/" -ForegroundColor Yellow
    Write-Host "After installation, restart your computer and run this script again." -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "[OK] Docker Desktop installed" -ForegroundColor Green
}

# Cài Minikube
if (!(Get-Command minikube -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Minikube..." -ForegroundColor Yellow
    choco install minikube -y
    Write-Host "[OK] Minikube installed" -ForegroundColor Green
} else {
    Write-Host "[OK] Minikube already installed" -ForegroundColor Green
}

# Cài kubectl
if (!(Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Host "Installing kubectl..." -ForegroundColor Yellow
    choco install kubernetes-cli -y
    Write-Host "[OK] kubectl installed" -ForegroundColor Green
} else {
    Write-Host "[OK] kubectl already installed" -ForegroundColor Green
}

# Cài Helm
if (!(Get-Command helm -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Helm..." -ForegroundColor Yellow
    choco install kubernetes-helm -y
    Write-Host "[OK] Helm installed" -ForegroundColor Green
} else {
    Write-Host "[OK] Helm already installed" -ForegroundColor Green
}

# Cài Python
if (!(Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Python..." -ForegroundColor Yellow
    choco install python311 -y
    Write-Host "[OK] Python installed" -ForegroundColor Green
} else {
    Write-Host "[OK] Python already installed" -ForegroundColor Green
}

Write-Host ""
Write-Host "=== Prerequisites Installation Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Close and reopen PowerShell to load new PATH" -ForegroundColor Yellow
Write-Host "2. Run: .\2-start-minikube.ps1" -ForegroundColor Yellow

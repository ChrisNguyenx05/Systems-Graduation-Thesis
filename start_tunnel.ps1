# ==============================================================================
# PUBLIC DEMO TUNNEL - CLOUDFLARE / LOCALTUNNEL / NGROK
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  START PUBLIC HTTPS DEMO TUNNEL (ZERO-COST CLOUD EXPOSURE)           " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Select the service you want to expose publicly to the Internet:" -ForegroundColor Yellow
Write-Host "  [1] NHFinance Admin Portal (Port 8090) [RECOMMENDED - ALL-IN-ONE]" -ForegroundColor Green
Write-Host "  [2] SYS-02: Apache Airflow (Port 8085)" -ForegroundColor White
Write-Host "  [3] SYS-05: MinIO S3 Console (Port 9001)" -ForegroundColor White
Write-Host "  [4] SYS-01: Streamlit Dashboard (Port 8501)" -ForegroundColor White
Write-Host "  [5] SYS-04: FastAPI Swagger Docs (Port 8000)" -ForegroundColor White
Write-Host "  [6] SYS-03: Grafana Monitoring (Port 3000)" -ForegroundColor White
Write-Host "  [7] Custom Port" -ForegroundColor White
Write-Host ""

$choice = Read-Host "Enter your selection (1-7) [Default: 1 - Admin Portal]"
if ([string]::IsNullOrWhiteSpace($choice)) { $choice = "1" }

$targetPort = 8090
$serviceName = "NHFinance Admin Portal"

switch ($choice) {
    "1" { $targetPort = 8090; $serviceName = "NHFinance Admin Portal" }
    "2" { $targetPort = 8085; $serviceName = "SYS-02 Airflow" }
    "3" { $targetPort = 9001; $serviceName = "SYS-05 MinIO Console" }
    "4" { $targetPort = 8501; $serviceName = "SYS-01 Streamlit Dashboard" }
    "5" { $targetPort = 8000; $serviceName = "SYS-04 FastAPI" }
    "6" { $targetPort = 3000; $serviceName = "SYS-03 Grafana" }
    "7" { 
        $custom = Read-Host "Enter port number to expose"
        $targetPort = [int]$custom
        $serviceName = "Custom Port $targetPort"
    }
    Default { $targetPort = 8090; $serviceName = "NHFinance Admin Portal" }
}

Write-Host ""
Write-Host "Preparing HTTPS tunnel for $serviceName on Port $targetPort..." -ForegroundColor Green

# 1. Try Cloudflare Tunnel (In PATH or local executable)
$cfBin = $null
if (Get-Command cloudflared -ErrorAction SilentlyContinue) {
    $cfBin = "cloudflared"
} elseif (Test-Path ".\cloudflared.exe") {
    $cfBin = ".\cloudflared.exe"
}

if ($cfBin) {
    Write-Host "Using Cloudflare Tunnel (100% Free, Secure & Fast)..." -ForegroundColor Cyan
    Write-Host "Your public HTTPS link (https://...trycloudflare.com) will appear below:" -ForegroundColor Yellow
    Write-Host ""
    & $cfBin tunnel --url "http://localhost:$targetPort"
    exit 0
}

# 2. Try Localtunnel via npx (Node.js)
$hasNpx = Get-Command npx -ErrorAction SilentlyContinue
if ($hasNpx) {
    Write-Host "Using Localtunnel via NPX (Node.js)..." -ForegroundColor Cyan
    Write-Host "Your public HTTPS link (https://...loca.lt) will appear below:" -ForegroundColor Yellow
    npx localtunnel --port $targetPort
    exit 0
}

# 3. Try Ngrok
$hasNgrok = Get-Command ngrok -ErrorAction SilentlyContinue
if ($hasNgrok) {
    Write-Host "Using Ngrok..." -ForegroundColor Cyan
    ngrok http $targetPort
    exit 0
}

# Neither tool installed
Write-Host "No tunnel CLI tool (cloudflared, localtunnel or ngrok) was detected." -ForegroundColor Yellow
Write-Host ""
Write-Host "QUICK INSTALLATION OPTIONS (CHOOSE ONLY ONE):" -ForegroundColor Magenta
Write-Host "  Option 1 (Recommended): Run in PowerShell: winget install Cloudflare.cloudflared" -ForegroundColor White
Write-Host "                          or download cloudflared.exe into this folder" -ForegroundColor White
Write-Host "  Option 2: Install Node.js to use built-in npx localtunnel" -ForegroundColor White
Write-Host "  Option 3: Download ngrok from https://ngrok.com/download" -ForegroundColor White
Write-Host ""

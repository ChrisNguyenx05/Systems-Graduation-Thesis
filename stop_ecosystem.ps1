# ==============================================================================
# 🛑 ENTERPRISE DATA & AI ECOSYSTEM - SHUTDOWN ORCHESTRATOR
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Magenta
Write-Host "  🛑 STOPPING AND RELEASING ALL ECOSYSTEM CONTAINERS & RESOURCES      " -ForegroundColor Magenta
Write-Host "======================================================================" -ForegroundColor Magenta
Write-Host ""

$composeCmd = "docker compose"
if (Get-Command "docker-compose" -ErrorAction SilentlyContinue) {
    $composeCmd = "docker-compose"
}

# 1. Stop Airflow (SYS-02)
if (Test-Path "SYS-02/docker-compose.yml") {
    Write-Host "[1/3] Stopping SYS-02 (Airflow Pipeline)..." -ForegroundColor Yellow
    Invoke-Expression "$composeCmd -f SYS-02/docker-compose.yml down"
    Write-Host "   ✅ SYS-02 stopped successfully." -ForegroundColor Green
}

# 2. Stop Lakehouse (SYS-05)
if (Test-Path "SYS-05/docker-compose.yml") {
    Write-Host ""
    Write-Host "[2/3] Stopping SYS-05 (Lakehouse: MinIO, ClickHouse, Postgres)..." -ForegroundColor Yellow
    Invoke-Expression "$composeCmd -f SYS-05/docker-compose.yml down"
    Write-Host "   ✅ SYS-05 stopped successfully." -ForegroundColor Green
}

# 3. Stop All Remaining Subsystems
$allServices = @("Admin_Portal", "WSIS", "SYS-04", "SYS-06", "SYS-01", "SYS-03")
Write-Host ""
Write-Host "[3/3] Stopping auxiliary microservices..." -ForegroundColor Yellow
foreach ($svc in $allServices) {
    if (Test-Path "$svc/docker-compose.yml") {
        Write-Host "   • Stopping $svc..." -ForegroundColor DarkGray
        Invoke-Expression "$composeCmd -f `"$svc/docker-compose.yml`" down"
    }
}

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Green
Write-Host "  🎉 ALL ECOSYSTEM SERVICES HAVE STOPPED SAFELY. RAM/CPU FREED!       " -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
Write-Host ""

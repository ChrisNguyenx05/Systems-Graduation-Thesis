# ==============================================================================
# 🚀 ENTERPRISE DATA & AI ECOSYSTEM - STARTUP ORCHESTRATOR
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  🌟 STARTING MINI-ENTERPRISE ECOSYSTEM (ALL-IN-ONE LOCAL RUNTIME)    " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Determine Compose Command (docker compose or docker-compose)
$composeCmd = "docker compose"
if (Get-Command "docker-compose" -ErrorAction SilentlyContinue) {
    $composeCmd = "docker-compose"
}

# 1. Check Docker Daemon Status
Write-Host "[1/5] Verifying Docker Desktop status..." -ForegroundColor Yellow
try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ ERROR: Docker Desktop is not running!" -ForegroundColor Red
        Write-Host "👉 Please start Docker Desktop and wait until the engine is fully running, then re-run this script." -ForegroundColor Magenta
        exit 1
    }
    Write-Host "   ✅ Docker Engine is running." -ForegroundColor Green
} catch {
    Write-Host "❌ ERROR: Docker CLI was not found in PATH!" -ForegroundColor Red
    exit 1
}

# 2. Check and Ensure Shared Docker Network
Write-Host ""
Write-Host "[2/5] Checking shared internal Docker network (thesis-network)..." -ForegroundColor Yellow
$networkExists = docker network ls --filter "name=^thesis-network$" --format "{{.Name}}"
if (-not $networkExists) {
    Write-Host "   ⚙️ Creating bridge network 'thesis-network'..." -ForegroundColor Cyan
    docker network create thesis-network | Out-Null
    Write-Host "   ✅ Network 'thesis-network' created successfully." -ForegroundColor Green
} else {
    Write-Host "   ✅ Network 'thesis-network' already exists." -ForegroundColor Green
}

# 3. Check and Auto-Bootstrap Missing .env Files
Write-Host ""
Write-Host "[3/5] Verifying environment configuration (.env files)..." -ForegroundColor Yellow
$envPairs = @(
    @{ Src = "SYS-05/.env.example"; Dst = "SYS-05/.env" },
    @{ Src = "SYS-02/.env.example"; Dst = "SYS-02/.env" },
    @{ Src = "SYS-04/.env.example"; Dst = "SYS-04/.env" },
    @{ Src = "SYS-06/.env.example"; Dst = "SYS-06/.env" },
    @{ Src = "WSIS/.env.example";   Dst = "WSIS/.env" },
    @{ Src = "Admin_Portal/backend/.env"; Dst = "Admin_Portal/backend/.env" }
)
foreach ($pair in $envPairs) {
    if (-not (Test-Path $pair.Dst) -and (Test-Path $pair.Src)) {
        Write-Host "   ⚙️ Creating missing $($pair.Dst) from $($pair.Src)..." -ForegroundColor Cyan
        Copy-Item $pair.Src $pair.Dst -Force
    }
}
Write-Host "   ✅ Environment files verified." -ForegroundColor Green

# 4. Start SYS-05: Storage & Lakehouse Core (MinIO, ClickHouse, Postgres)
Write-Host ""
Write-Host "[4/5] Launching SYS-05: Lakehouse & Database Management Portal..." -ForegroundColor Yellow
if (Test-Path "SYS-05/docker-compose.yml") {
    Invoke-Expression "$composeCmd -f SYS-05/docker-compose.yml up -d"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✅ SYS-05 Containers (MinIO, ClickHouse, Postgres) started successfully." -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Warning: SYS-05 encountered an issue during startup." -ForegroundColor Red
    }
}

# 5. Start SYS-02: Apache Airflow (v3.3 Pipeline Orchestration)
Write-Host ""
Write-Host "[5/5] Launching SYS-02: Data Pipeline & Orchestration (Airflow v3.3)..." -ForegroundColor Yellow
Start-Sleep -Seconds 3
if (Test-Path "SYS-02/docker-compose.yml") {
    Invoke-Expression "$composeCmd -f SYS-02/docker-compose.yml up -d"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✅ SYS-02 Airflow (Webserver/API, Scheduler, Dag Processor) started." -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Warning: SYS-02 encountered an issue during startup." -ForegroundColor Red
    }
}

# Start remaining subsystems if compose file is present
$otherServices = @("WSIS", "SYS-04", "SYS-06", "Admin_Portal")
foreach ($svc in $otherServices) {
    if (Test-Path "$svc/docker-compose.yml") {
        Write-Host ""
        Write-Host "Starting $svc..." -ForegroundColor Yellow
        Invoke-Expression "$composeCmd -f `"$svc/docker-compose.yml`" up -d"
    }
}

# Summary and Browser Launch
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  🎉 ECOSYSTEM IS READY FOR DEMO & TESTING                           " -ForegroundColor Green
Write-Host "  👉 Unified Portal:     file://$((Get-Location).Path.Replace('\','/'))/index.html" -ForegroundColor White
Write-Host "  👉 Admin Portal:       http://localhost:8090 (nhfinanceadmin / nhathuy123)" -ForegroundColor White
Write-Host "  👉 SYS-04 AI Serving:  http://localhost:8000/docs" -ForegroundColor White
Write-Host "  👉 SYS-02 Airflow UI:  http://localhost:8085 (nhfinanceadmin / nhathuy123)" -ForegroundColor White
Write-Host "  👉 SYS-05 MinIO UI:    http://localhost:9001 (nhfinanceadmin / nhathuy123)" -ForegroundColor White
Write-Host "  👉 SYS-05 ClickHouse:  http://localhost:8123/play" -ForegroundColor White
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Open Portal in default browser
Start-Process "index.html"

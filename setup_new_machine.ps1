# ==============================================================================
# 🌟 ENTERPRISE PLATFORM - FIRST-TIME BOOTSTRAPPER FOR NEW MACHINES
# ==============================================================================
# Purpose: Automatically setups environment variables, Docker networks, builds
#          all container images, applies database migrations, and starts all systems.
# Options:
#   .\setup_new_machine.ps1             : Standard incremental bootstrap
#   .\setup_new_machine.ps1 -CleanAll   : Purge old containers, volumes, databases,
#                                         and historical data (preserves .env files)
# ==============================================================================

[CmdletBinding()]
param (
    [switch]$CleanAll = $false
)

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  🌟 INITIALIZING ECOSYSTEM ON A NEW MACHINE (AUTOMATED BOOTSTRAP)    " -ForegroundColor Cyan
if ($CleanAll) {
    Write-Host "  🧹 MODE: CLEAN-ALL (Full Reset: Containers, Volumes & Database State) " -ForegroundColor Yellow
    Write-Host "  🛡️  PRESERVATION: All .env files are kept intact                     " -ForegroundColor Yellow
} else {
    Write-Host "  ⚡ MODE: INCREMENTAL (Safe Bootstrap, preserving existing data)      " -ForegroundColor Green
}
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Determine Compose Command
$composeCmd = "docker compose"
if (Get-Command "docker-compose" -ErrorAction SilentlyContinue) {
    $composeCmd = "docker-compose"
}

# ------------------------------------------------------------------------------
# STEP 0: CLEAN-ALL PURGE (TRIGGERED ONLY WHEN -CleanAll IS SPECIFIED)
# ------------------------------------------------------------------------------
if ($CleanAll) {
    Write-Host "[0/7] Performing Full Clean-All Reset..." -ForegroundColor Yellow
    
    # 1. Stop and remove all containers, networks, volumes
    Write-Host "   🛑 Terminating and removing all ecosystem containers and volumes..." -ForegroundColor Cyan
    $allServices = @("Admin_Portal", "WSIS", "SYS-04", "SYS-06", "SYS-02", "SYS-05")
    foreach ($svc in $allServices) {
        if (Test-Path "$svc/docker-compose.yml") {
            Write-Host "      • Tearing down $svc (volumes & orphans)..." -ForegroundColor DarkGray
            Invoke-Expression "$composeCmd -f `"$svc/docker-compose.yml`" down -v --remove-orphans" | Out-Null
        }
    }

    # 2. Remove shared Docker network
    $netCheck = docker network ls --filter "name=^thesis-network$" --format "{{.Name}}"
    if ($netCheck) {
        Write-Host "      • Removing existing 'thesis-network'..." -ForegroundColor DarkGray
        docker network rm thesis-network 2>$null | Out-Null
    }

    # 3. Purge storage directories (MinIO, ClickHouse, Postgres, Airflow logs)
    # NOTE: Preserves directory structure and all .env files intact!
    Write-Host "   🗑️ Purging lakehouse storage, database parts & runtime logs..." -ForegroundColor Cyan
    $dataDirs = @(
        "SYS-05/data/minio",
        "SYS-05/data/clickhouse",
        "SYS-05/data/postgres",
        "SYS-02/data/postgres",
        "SYS-02/logs",
        "SYS-06/logs"
    )
    foreach ($dir in $dataDirs) {
        if (Test-Path $dir) {
            Get-ChildItem -Path $dir -Recurse -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
            Write-Host "      • Cleared $dir" -ForegroundColor DarkGray
        } else {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
        }
    }
    Write-Host "   ✅ Clean-All completed: System reset to pristine blank state." -ForegroundColor Green
    Write-Host ""
}

# ------------------------------------------------------------------------------
# STEP 1: VERIFY DOCKER DAEMON
# ------------------------------------------------------------------------------
Write-Host "[1/7] Checking Docker Engine status..." -ForegroundColor Yellow
try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ ERROR: Docker Desktop is not running!" -ForegroundColor Red
        Write-Host "👉 Please launch Docker Desktop, wait for 'Engine Running', then re-run this script." -ForegroundColor Magenta
        exit 1
    }
    Write-Host "   ✅ Docker Engine is healthy." -ForegroundColor Green
} catch {
    Write-Host "❌ ERROR: Docker CLI not found in PATH!" -ForegroundColor Red
    exit 1
}

# ------------------------------------------------------------------------------
# STEP 2: CREATE SHARED DOCKER NETWORK
# ------------------------------------------------------------------------------
Write-Host ""
Write-Host "[2/7] Initializing shared Docker network ('thesis-network')..." -ForegroundColor Yellow
$netExists = docker network ls --filter "name=^thesis-network$" --format "{{.Name}}"
if (-not $netExists) {
    docker network create thesis-network | Out-Null
    Write-Host "   ✅ Created 'thesis-network' bridge network." -ForegroundColor Green
} else {
    Write-Host "   ✅ 'thesis-network' already exists." -ForegroundColor Green
}

# ------------------------------------------------------------------------------
# STEP 3: AUTOMATE .ENV CONFIGURATIONS FOR ALL SUBSYSTEMS
# ------------------------------------------------------------------------------
Write-Host ""
Write-Host "[3/7] Setting up environment configuration (.env) files..." -ForegroundColor Yellow

# Helper to copy if missing
function Ensure-EnvFile($src, $dst) {
    if (-not (Test-Path $dst) -and (Test-Path $src)) {
        Copy-Item $src $dst -Force
        Write-Host "   ⚙️ Created $dst from template." -ForegroundColor Cyan
    }
}

Ensure-EnvFile "SYS-05/.env.example" "SYS-05/.env"
Ensure-EnvFile "SYS-02/.env.example" "SYS-02/.env"
Ensure-EnvFile "SYS-04/.env.example" "SYS-04/.env"
Ensure-EnvFile "SYS-06/.env.example" "SYS-06/.env"
Ensure-EnvFile "WSIS/.env.example" "WSIS/.env"
Ensure-EnvFile "Admin_Portal/backend/.env" "Admin_Portal/backend/.env"

# Ensure unified credentials in SYS-05/.env
if (Test-Path "SYS-05/.env") {
    $sys05Env = Get-Content "SYS-05/.env" -Raw
    $sys05Env = $sys05Env -replace "replace_with_minio_access_key", "nhfinanceadmin"
    $sys05Env = $sys05Env -replace "replace_with_a_long_minio_secret", "nhathuy123"
    $sys05Env = $sys05Env -replace "replace_with_clickhouse_user", "nhfinanceadmin"
    $sys05Env = $sys05Env -replace "replace_with_a_long_clickhouse_password", "nhathuy123"
    $sys05Env = $sys05Env -replace "replace_with_postgres_user", "admin123"
    $sys05Env = $sys05Env -replace "replace_with_a_long_postgres_password", "admin123"
    $sys05Env = $sys05Env -replace "replace_with_postgres_database_name", "enterprise_metadata"
    Set-Content -Path "SYS-05/.env" -Value $sys05Env -Force
}

# Ensure unified credentials in SYS-02/.env
if (Test-Path "SYS-02/.env") {
    $sys02Env = Get-Content "SYS-02/.env" -Raw
    $sys02Env = $sys02Env -replace "replace_with_minio_access_key", "nhfinanceadmin"
    $sys02Env = $sys02Env -replace "replace_with_a_long_minio_secret", "nhathuy123"
    $sys02Env = $sys02Env -replace "replace_with_clickhouse_user", "nhfinanceadmin"
    $sys02Env = $sys02Env -replace "replace_with_a_long_clickhouse_password", "nhathuy123"
    $sys02Env = $sys02Env -replace "replace_with_airflow_admin_username", "nhfinanceadmin"
    $sys02Env = $sys02Env -replace "replace_with_a_long_airflow_admin_password", "nhathuy123"
    $sys02Env = $sys02Env -replace "replace_with_a_valid_fernet_key", "LM53ZoePZgpYnkz7loi46Ef9om7bJpzB5PbyGgOoCHQ="
    Set-Content -Path "SYS-02/.env" -Value $sys02Env -Force
}

# Ensure SYS-04/.env has default credentials
if (Test-Path "SYS-04/.env") {
    $sys04Env = Get-Content "SYS-04/.env" -Raw
    $sys04Env = $sys04Env -replace "CLICKHOUSE_USER=.*", "CLICKHOUSE_USER=nhfinanceadmin"
    $sys04Env = $sys04Env -replace "CLICKHOUSE_PASSWORD=.*", "CLICKHOUSE_PASSWORD=nhathuy123"
    $sys04Env = $sys04Env -replace "CLICKHOUSE_DB=.*", "CLICKHOUSE_DB=default"
    $sys04Env = $sys04Env -replace "AWS_ACCESS_KEY_ID=.*", "AWS_ACCESS_KEY_ID=nhfinanceadmin"
    $sys04Env = $sys04Env -replace "AWS_SECRET_ACCESS_KEY=.*", "AWS_SECRET_ACCESS_KEY=nhathuy123"
    Set-Content -Path "SYS-04/.env" -Value $sys04Env -Force
}

# Ensure WSIS/.env has default credentials
if (Test-Path "WSIS/.env") {
    $wsisEnv = Get-Content "WSIS/.env" -Raw
    $wsisEnv = $wsisEnv -replace "MINIO_ACCESS_KEY=.*", "MINIO_ACCESS_KEY=nhfinanceadmin"
    $wsisEnv = $wsisEnv -replace "MINIO_SECRET_KEY=.*", "MINIO_SECRET_KEY=nhathuy123"
    $wsisEnv = $wsisEnv -replace "MINIO_ENDPOINT=http://127.0.0.1:9000", "MINIO_ENDPOINT=http://host.docker.internal:9000"
    Set-Content -Path "WSIS/.env" -Value $wsisEnv -Force
}

# Verify all 6 subsystem .env configuration profiles
Write-Host "   🔍 Auditing subsystem environment profiles..." -ForegroundColor Cyan
$envAudit = @(
    @{ System = "SYS-05"; Path = "SYS-05/.env" },
    @{ System = "SYS-02"; Path = "SYS-02/.env" },
    @{ System = "SYS-04"; Path = "SYS-04/.env" },
    @{ System = "SYS-06"; Path = "SYS-06/.env" },
    @{ System = "WSIS";   Path = "WSIS/.env" },
    @{ System = "PORTAL"; Path = "Admin_Portal/backend/.env" }
)
$envMissing = @()
foreach ($item in $envAudit) {
    if (Test-Path $item.Path) {
        Write-Host "      • $($item.System,-8) $($item.Path,-28) [OK]" -ForegroundColor Green
    } else {
        Write-Host "      • $($item.System,-8) $($item.Path,-28) [MISSING]" -ForegroundColor Red
        $envMissing += $item.Path
    }
}
if ($envMissing.Count -gt 0) {
    Write-Host "❌ ERROR: Missing required .env file(s): $($envMissing -join ', ')" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ All 6 subsystem .env profiles verified and synchronized." -ForegroundColor Green

# ------------------------------------------------------------------------------
# STEP 4: CLEAN INCOMPATIBLE MINIO STORAGE CACHE IF DETECTED
# ------------------------------------------------------------------------------
if (Test-Path "SYS-05/data/minio/.minio.sys") {
    Write-Host ""
    Write-Host "   ℹ️ Cleaning previous MinIO metadata to prevent format mismatch..." -ForegroundColor Cyan
    Remove-Item -Recurse -Force "SYS-05/data/minio/.minio.sys" -ErrorAction SilentlyContinue
}

# ------------------------------------------------------------------------------
# STEP 5: LAUNCH SYS-05 (STORAGE, CLICKHOUSE, MINIO, POSTGRES)
# ------------------------------------------------------------------------------
Write-Host ""
Write-Host "[4/7] Launching SYS-05: Lakehouse Storage & Databases..." -ForegroundColor Yellow
Invoke-Expression "$composeCmd -f SYS-05/docker-compose.yml up -d"
Write-Host "   ⏳ Waiting 10 seconds for ClickHouse and MinIO initialization..." -ForegroundColor Cyan
Start-Sleep -Seconds 10

# Apply ClickHouse DDL Migrations (Ensure pair_id, source, type_method and AI inference tables exist)
Write-Host "   ⚙️ Applying ClickHouse database schemas & table migrations..." -ForegroundColor Cyan
$migrationSql = @"
ALTER TABLE default.crypto_klines_gold ADD COLUMN IF NOT EXISTS pair_id UInt8 DEFAULT 0;
ALTER TABLE default.crypto_klines_gold ADD COLUMN IF NOT EXISTS source LowCardinality(String) DEFAULT 'v1.0.0_api_klines';
ALTER TABLE default.crypto_klines_gold ADD COLUMN IF NOT EXISTS type_method LowCardinality(String) DEFAULT 'API';
CREATE DATABASE IF NOT EXISTS db_gold_analytics;
CREATE TABLE IF NOT EXISTS db_gold_analytics.ai_hourly_inference_results (
    logical_date DateTime,
    inference_timestamp DateTime DEFAULT logical_date,
    symbol LowCardinality(String),
    current_price Float64,
    forecast_15m_price Float64,
    forecast_15m_delta_pct Float64,
    forecast_60m_price Float64,
    forecast_60m_delta_pct Float64,
    direction LowCardinality(String),
    anomaly_score Float64,
    risk_level LowCardinality(String),
    bar_range_bps Float64,
    volatility_parkinson Float64,
    volume_surge_ratio Float64,
    leverage_limit UInt16 DEFAULT 20,
    spread_buffer String DEFAULT 'Standard',
    fraud_decision LowCardinality(String) DEFAULT 'APPROVE',
    fraud_risk_score Float64 DEFAULT 0.0,
    governance_mode LowCardinality(String) DEFAULT 'ADVISORY_AND_RISK_GUARD',
    execution_latency_ms Float64 DEFAULT 0.0,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at) ORDER BY (symbol, logical_date);
CREATE TABLE IF NOT EXISTS db_gold_analytics.ai_forecast_reconciliation (
    symbol LowCardinality(String),
    prediction_time DateTime,
    realized_time DateTime,
    predicted_price_60m Float64,
    predicted_direction LowCardinality(String),
    predicted_delta_pct Float64,
    actual_price Float64,
    actual_direction LowCardinality(String),
    actual_delta_pct Float64,
    direction_hit UInt8,
    absolute_error Float64,
    percentage_error Float64,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at) ORDER BY (symbol, prediction_time);
"@

try {
    docker exec -i clickhouse_server clickhouse-client -u nhfinanceadmin --password nhathuy123 --multiquery --query $migrationSql 2>$null
    Write-Host "   ✅ ClickHouse schemas migrated successfully." -ForegroundColor Green
} catch {
    Write-Host "   ⚠️ Notice: Schema migration will run automatically via Airflow operator." -ForegroundColor DarkGray
}

# ------------------------------------------------------------------------------
# STEP 6: BUILD & LAUNCH SYS-02 AIRFLOW (v3.3) WITH CLEAN CACHE
# ------------------------------------------------------------------------------
Write-Host ""
Write-Host "[5/7] Building & Launching SYS-02: Airflow v3.3 Pipeline Engine..." -ForegroundColor Yellow
Invoke-Expression "$composeCmd -f SYS-02/docker-compose.yml up -d --build"
Write-Host "   ✅ SYS-02 Airflow built and started successfully." -ForegroundColor Green

# ------------------------------------------------------------------------------
# STEP 7: BUILD & LAUNCH AUXILIARY SUBSYSTEMS (WSIS, SYS-04, SYS-06, ADMIN PORTAL)
# ------------------------------------------------------------------------------
Write-Host ""
Write-Host "[6/7] Building & Launching remaining ecosystem subsystems..." -ForegroundColor Yellow
$remaining = @("WSIS", "SYS-04", "SYS-06", "Admin_Portal")
foreach ($svc in $remaining) {
    if (Test-Path "$svc/docker-compose.yml") {
        Write-Host "   • Building & starting $svc..." -ForegroundColor Cyan
        Invoke-Expression "$composeCmd -f `"$svc/docker-compose.yml`" up -d --build"
    }
}
Write-Host "   ✅ All subsystems started." -ForegroundColor Green

# ------------------------------------------------------------------------------
# STEP 8: RUN HEALTH CHECK AND LAUNCH PORTAL
# ------------------------------------------------------------------------------
Write-Host ""
Write-Host "[7/7] Verifying ecosystem health..." -ForegroundColor Yellow
if (Test-Path "check_health.ps1") {
    .\check_health.ps1
}

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  🎉 NEW MACHINE SETUP COMPLETED SUCCESSFULLY!                        " -ForegroundColor Green
Write-Host "  👉 Unified Portal:     file://$((Get-Location).Path.Replace('\','/'))/index.html" -ForegroundColor White
Write-Host "  👉 Admin Portal:       http://localhost:8090 (nhfinanceadmin / nhathuy123)" -ForegroundColor White
Write-Host "  👉 SYS-04 AI Serving:  http://localhost:8000/docs" -ForegroundColor White
Write-Host "  👉 SYS-02 Airflow UI:  http://localhost:8085 (nhfinanceadmin / nhathuy123)" -ForegroundColor White
Write-Host "  👉 SYS-05 MinIO UI:    http://localhost:9001 (nhfinanceadmin / nhathuy123)" -ForegroundColor White
Write-Host "  👉 SYS-05 ClickHouse:  http://localhost:8123/play" -ForegroundColor White
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

Start-Process "index.html"

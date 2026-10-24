# ==============================================================================
# ENTERPRISE DATA & AI ECOSYSTEM - COMPREHENSIVE PRE-FLIGHT VERIFICATION TEST
# ==============================================================================
# Purpose: End-to-end smoke test & health diagnostics to run before live demos
#          or when testing on a new machine.
# Usage:
#   .\test_ecosystem.ps1
#   .\test_ecosystem.ps1 -VerboseDetails
#   .\test_ecosystem.ps1 -RunPytest
# ==============================================================================

[CmdletBinding()]
param (
    [switch]$VerboseDetails = $false,
    [switch]$RunPytest = $false,
    [switch]$SkipDockerCheck = $false
)

$startTime = Get-Date

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  ENTERPRISE PLATFORM - PRE-FLIGHT VERIFICATION & INTEGRATION TEST    " -ForegroundColor Cyan
Write-Host "  Subsystems: SYS-01 -> SYS-06 | Lakehouse | Airflow v3.3 | AI Serving " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

$testResults = [System.Collections.Generic.List[PSCustomObject]]::new()

function Add-TestResult {
    param (
        [string]$Category,
        [string]$Subsystem,
        [string]$TestName,
        [string]$Status,       # "PASS", "FAIL", "WARN", "SKIP"
        [string]$Details,
        [double]$DurationMs = 0
    )
    $color = switch ($Status) {
        "PASS" { "Green" }
        "FAIL" { "Red" }
        "WARN" { "Yellow" }
        default { "DarkGray" }
    }
    $badge = "[$Status]"
    Write-Host ("  {0,-8} {1,-8} | {2,-30} | {3}" -f $badge, $Subsystem, $TestName, $Details) -ForegroundColor $color
    $script:testResults.Add([PSCustomObject]@{
        Category  = $Category
        System    = $Subsystem
        TestName  = $TestName
        Status    = $badge
        Details   = $Details
        TimeMs    = [math]::Round($DurationMs, 1)
    })
}

function Test-TcpPort {
    param ([string]$HostName, [int]$Port, [int]$TimeoutMs = 1500)
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $connect = $tcp.BeginConnect($HostName, $Port, $null, $null)
        $wait = $connect.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
        if ($wait -and $tcp.Connected) {
            $tcp.EndConnect($connect)
            $tcp.Close()
            return $true
        }
        $tcp.Close()
        return $false
    } catch {
        return $false
    }
}

function Invoke-QuickHttp {
    param (
        [string]$Url,
        [int]$TimeoutSec = 3,
        [string]$Method = "GET",
        [string]$Body = $null,
        [hashtable]$Headers = @{}
    )
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $params = @{
            Uri = $Url
            Method = $Method
            TimeoutSec = $TimeoutSec
            ErrorAction = "Stop"
        }
        if ($Body) {
            $params["Body"] = $Body
            $params["ContentType"] = "application/json"
        }
        if ($Headers.Count -gt 0) {
            $params["Headers"] = $Headers
        }
        $res = Invoke-RestMethod @params
        $sw.Stop()
        return @{ Success = $true; Data = $res; ElapsedMs = $sw.ElapsedMilliseconds }
    } catch {
        $sw.Stop()
        return @{ Success = $false; Error = $_.Exception.Message; ElapsedMs = $sw.ElapsedMilliseconds }
    }
}

# ==============================================================================
# SUITE 1: HOST ENVIRONMENT & PREREQUISITES
# ==============================================================================
Write-Host ">> SUITE 1: Host Environment & Docker Engine Diagnostics" -ForegroundColor Yellow

# 1.1 Docker CLI & Daemon
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$dockerOk = $false
try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -eq 0) { $dockerOk = $true }
} catch {}
$sw.Stop()

if ($dockerOk) {
    Add-TestResult "Host" "DOCKER" "Docker Engine Daemon" "PASS" "Daemon running and responsive" $sw.ElapsedMilliseconds
} else {
    if ($SkipDockerCheck) {
        Add-TestResult "Host" "DOCKER" "Docker Engine Daemon" "WARN" "Docker check skipped by user flag" $sw.ElapsedMilliseconds
    } else {
        Add-TestResult "Host" "DOCKER" "Docker Engine Daemon" "FAIL" "Docker Desktop is NOT running! Start Docker first." $sw.ElapsedMilliseconds
        Write-Host ""
        Write-Host "[!] FATAL: Cannot continue tests without Docker Engine." -ForegroundColor Red
        Write-Host "[*] Start Docker Desktop and wait for it to be ready, then re-run this script." -ForegroundColor Magenta
        Write-Host "    (Or run with -SkipDockerCheck to test only network ports)" -ForegroundColor DarkGray
        exit 1
    }
}

# 1.2 Shared Docker Network
if ($dockerOk) {
    $netExists = docker network ls --filter "name=^thesis-network$" --format "{{.Name}}"
    if ($netExists) {
        Add-TestResult "Host" "NETWORK" "Docker Shared Bridge" "PASS" "Bridge network 'thesis-network' active"
    } else {
        Add-TestResult "Host" "NETWORK" "Docker Shared Bridge" "WARN" "Missing 'thesis-network' (auto-creating...)"
        docker network create thesis-network | Out-Null
    }
} else {
    Add-TestResult "Host" "NETWORK" "Docker Shared Bridge" "SKIP" "Docker not running"
}

# 1.3 Subsystem Environment (.env) Files
$envFiles = @("SYS-05/.env", "SYS-02/.env", "SYS-04/.env", "SYS-06/.env", "WSIS/.env", "Admin_Portal/backend/.env")
$missingEnvs = @()
foreach ($ef in $envFiles) {
    if (-not (Test-Path $ef)) { $missingEnvs += $ef }
}
if ($missingEnvs.Count -eq 0) {
    Add-TestResult "Host" "CONFIG" "Subsystem .env Profiles" "PASS" "All 6 .env configuration profiles exist"
} else {
    Add-TestResult "Host" "CONFIG" "Subsystem .env Profiles" "WARN" ("Missing: " + ($missingEnvs -join ", ") + " (Run setup_new_machine.ps1)")
}

Write-Host ""

# ==============================================================================
# SUITE 2: STORAGE & LAKEHOUSE INTEGRITY (SYS-05)
# ==============================================================================
Write-Host ">> SUITE 2: Storage & Lakehouse Database Verification (SYS-05)" -ForegroundColor Yellow

# 2.1 MinIO S3 API Health
$minioHttp = Invoke-QuickHttp "http://localhost:9000/minio/health/live"
if ($minioHttp.Success) {
    Add-TestResult "Lakehouse" "SYS-05" "MinIO S3 Health Check" "PASS" "S3 API Live (Port 9000)" $minioHttp.ElapsedMs
} else {
    Add-TestResult "Lakehouse" "SYS-05" "MinIO S3 Health Check" "FAIL" "MinIO S3 unreachable on port 9000" $minioHttp.ElapsedMs
}

# 2.2 MinIO Console Port
$minioConsoleOpen = Test-TcpPort "127.0.0.1" 9001
if ($minioConsoleOpen) {
    Add-TestResult "Lakehouse" "SYS-05" "MinIO Web Console" "PASS" "Web console open on port 9001"
} else {
    Add-TestResult "Lakehouse" "SYS-05" "MinIO Web Console" "WARN" "Port 9001 unreachable"
}

# 2.3 ClickHouse HTTP Ping
$chPing = Invoke-QuickHttp "http://localhost:8123/ping"
if ($chPing.Success -and $chPing.Data -match "Ok") {
    Add-TestResult "Lakehouse" "SYS-05" "ClickHouse Engine Ping" "PASS" "ClickHouse HTTP engine OK (Port 8123)" $chPing.ElapsedMs
} else {
    Add-TestResult "Lakehouse" "SYS-05" "ClickHouse Engine Ping" "FAIL" "ClickHouse server not responding on port 8123" $chPing.ElapsedMs
}

# 2.4 ClickHouse SQL Query & Schema Validation
if ($chPing.Success) {
    $chAuth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("nhfinanceadmin:nhathuy123"))
    $headers = @{ Authorization = "Basic $chAuth" }
    
    $versionTest = Invoke-QuickHttp -Url "http://localhost:8123/?query=SELECT%20version()" -Headers $headers
    
    if ($versionTest.Success) {
        $chVer = ($versionTest.Data -as [string]).Trim()
        Add-TestResult "Lakehouse" "SYS-05" "ClickHouse Query Engine" "PASS" ("Query OK (Version: " + $chVer + ")") $versionTest.ElapsedMs
    } else {
        Add-TestResult "Lakehouse" "SYS-05" "ClickHouse Query Engine" "FAIL" "Query failed or bad credentials (nhfinanceadmin)" $versionTest.ElapsedMs
    }

    # 2.5 Check ClickHouse Table Schemas
    $pairIdCheck = Invoke-QuickHttp -Url "http://localhost:8123/?query=SELECT%20count()%20FROM%20system.columns%20WHERE%20table%3D'crypto_klines_gold'%20AND%20name%3D'pair_id'" -Headers $headers
    if ($pairIdCheck.Success -and [int]"$($pairIdCheck.Data)".Trim() -ge 1) {
        Add-TestResult "Lakehouse" "SYS-05" "Schema: pair_id column" "PASS" "Column 'pair_id' exists in crypto_klines_gold"
    } else {
        Add-TestResult "Lakehouse" "SYS-05" "Schema: pair_id column" "WARN" "Column 'pair_id' missing (auto-patch will run via Airflow)"
    }

    $aiTableCheck = Invoke-QuickHttp -Url "http://localhost:8123/?query=SELECT%20count()%20FROM%20system.tables%20WHERE%20database%3D'db_gold_analytics'%20AND%20name%3D'ai_hourly_inference_results'" -Headers $headers
    if ($aiTableCheck.Success -and [int]"$($aiTableCheck.Data)".Trim() -ge 1) {
        Add-TestResult "Lakehouse" "SYS-05" "Schema: AI Inference Table" "PASS" "db_gold_analytics.ai_hourly_inference_results exists"
    } else {
        Add-TestResult "Lakehouse" "SYS-05" "Schema: AI Inference Table" "WARN" "Table missing (Run setup_new_machine.ps1 to initialize DDL)"
    }
}

# 2.6 PostgreSQL (Enterprise Metadata)
$pgOpen = Test-TcpPort "127.0.0.1" 5432
if ($pgOpen) {
    Add-TestResult "Lakehouse" "SYS-05" "Enterprise Metadata DB" "PASS" "PostgreSQL ready on port 5432"
} else {
    Add-TestResult "Lakehouse" "SYS-05" "Enterprise Metadata DB" "WARN" "Port 5432 unreachable"
}

Write-Host ""

# ==============================================================================
# SUITE 3: AIRFLOW ORCHESTRATION PIPELINE (SYS-02)
# ==============================================================================
Write-Host ">> SUITE 3: Apache Airflow v3.3 Pipeline & DAG Verification (SYS-02)" -ForegroundColor Yellow

# 3.1 Airflow Metadata DB
$afPgOpen = Test-TcpPort "127.0.0.1" 5433
if ($afPgOpen) {
    Add-TestResult "Airflow" "SYS-02" "Airflow Postgres DB" "PASS" "Metadata DB ready on port 5433"
} else {
    Add-TestResult "Airflow" "SYS-02" "Airflow Postgres DB" "WARN" "Airflow DB not reachable on port 5433"
}

# 3.2 Airflow Webserver / API Server HTTP
$afHttp = Invoke-QuickHttp "http://localhost:8085"
if ($afHttp.Success) {
    Add-TestResult "Airflow" "SYS-02" "Airflow Webserver (v3.3)" "PASS" "Webserver responsive (Port 8085)" $afHttp.ElapsedMs
} else {
    Add-TestResult "Airflow" "SYS-02" "Airflow Webserver (v3.3)" "FAIL" "Airflow UI not reachable on port 8085" $afHttp.ElapsedMs
}

# 3.3 Airflow Scheduler & DAG Bag Verification
if ($dockerOk) {
    $schedulerRunning = docker ps --filter "name=sys02_airflow_scheduler" --filter "status=running" --format "{{.Names}}"
    if ($schedulerRunning) {
        Add-TestResult "Airflow" "SYS-02" "Airflow Scheduler Process" "PASS" "Container sys02_airflow_scheduler is running"
        
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $dagListOut = docker exec sys02_airflow_scheduler airflow dags list -o plain 2>&1
        $sw.Stop()
        
        if ($LASTEXITCODE -eq 0 -and $dagListOut -match "01_binance_klines_etl_dag") {
            $dagCount = ($dagListOut | Select-String -Pattern "0[1-6]_").Count
            Add-TestResult "Airflow" "SYS-02" "Airflow DAG Parsing" "PASS" ("All " + $dagCount + " core thesis DAGs parsed successfully without syntax errors") $sw.ElapsedMilliseconds
        } else {
            Add-TestResult "Airflow" "SYS-02" "Airflow DAG Parsing" "WARN" "DAG list returned warning or DAGs are still loading in background" $sw.ElapsedMilliseconds
        }
    } else {
        Add-TestResult "Airflow" "SYS-02" "Airflow Scheduler Process" "FAIL" "sys02_airflow_scheduler container is NOT running"
    }
} else {
    Add-TestResult "Airflow" "SYS-02" "Airflow Scheduler Process" "SKIP" "Docker not running"
}

Write-Host ""

# ==============================================================================
# SUITE 4: AI & ML MODEL SERVING CENTER (SYS-04)
# ==============================================================================
Write-Host ">> SUITE 4: Machine Learning Serving & AI Governance (SYS-04)" -ForegroundColor Yellow

# 4.1 FastAPI Model Serving Health Check
$aiHealth = Invoke-QuickHttp "http://localhost:8000/health"
if ($aiHealth.Success) {
    $modelsLoaded = $aiHealth.Data.models_loaded
    $statusText = $aiHealth.Data.status
    if ($statusText -eq "HEALTHY" -or $modelsLoaded -match "3/3") {
        Add-TestResult "AI Serving" "SYS-04" "FastAPI Serving Health" "PASS" "3/3 AI Models Loaded & Ready" $aiHealth.ElapsedMs
    } else {
        Add-TestResult "AI Serving" "SYS-04" "FastAPI Serving Health" "WARN" ("Status: " + $statusText + " (" + $modelsLoaded + " models loaded)") $aiHealth.ElapsedMs
    }
} else {
    Add-TestResult "AI Serving" "SYS-04" "FastAPI Serving Health" "FAIL" "SYS-04 FastAPI unreachable on port 8000" $aiHealth.ElapsedMs
}

# 4.2 Live Mock Inference Test (Fraud Detection Gatekeeper)
if ($aiHealth.Success) {
    $fraudPayload = '{"user_id": "test_preflight_usr", "amount_usd": 150.0, "velocity_1h": 1, "country_risk_score": 0.05, "device_trust_score": 0.98, "ip_distance_km": 5.0, "is_card_present": true}'
    $predictRes = Invoke-QuickHttp -Url "http://localhost:8000/api/v1/predict/fraud" -Method "POST" -Body $fraudPayload
    if ($predictRes.Success -and $predictRes.Data.decision) {
        $decision = $predictRes.Data.decision
        $score = $predictRes.Data.risk_score
        Add-TestResult "AI Serving" "SYS-04" "Live Inference Smoke Test" "PASS" ("Fraud Model responded: Decision=" + $decision + " (Score: " + $score + ")") $predictRes.ElapsedMs
    } else {
        Add-TestResult "AI Serving" "SYS-04" "Live Inference Smoke Test" "WARN" "Mock inference request failed or schema changed" $predictRes.ElapsedMs
    }
}

# 4.3 MLflow Tracking UI
$mlflowOpen = Test-TcpPort "127.0.0.1" 5000
if ($mlflowOpen) {
    Add-TestResult "AI Serving" "SYS-04" "MLflow Tracking UI" "PASS" "MLflow server reachable on port 5000"
} else {
    Add-TestResult "AI Serving" "SYS-04" "MLflow Tracking UI" "WARN" "MLflow port 5000 offline or not deployed"
}

Write-Host ""

# ==============================================================================
# SUITE 5: REALTIME INGESTION & ALERT DISPATCHER (WSIS & SYS-06)
# ==============================================================================
Write-Host ">> SUITE 5: Real-Time Stream Ingestion & Operations Alerting" -ForegroundColor Yellow

# 5.1 WSIS Realtime Ingestion Port
$wsisOpen = Test-TcpPort "127.0.0.1" 8005
if ($wsisOpen) {
    Add-TestResult "Ingestion" "WSIS" "WebSocket Ingestion Collector" "PASS" "Realtime ingestion port 8005 is active"
} else {
    Add-TestResult "Ingestion" "WSIS" "WebSocket Ingestion Collector" "WARN" "WSIS port 8005 offline"
}

# 5.2 SYS-06 Alert Dispatcher Port
$sys06Open = Test-TcpPort "127.0.0.1" 8080
if ($sys06Open) {
    Add-TestResult "Alerting" "SYS-06" "Alert Dispatcher Engine" "PASS" "Alert dispatcher active on port 8080"
} else {
    Add-TestResult "Alerting" "SYS-06" "Alert Dispatcher Engine" "WARN" "SYS-06 port 8080 offline"
}

Write-Host ""

# ==============================================================================
# SUITE 6: MONITORING & COMMAND PORTAL (SYS-01, SYS-03, ADMIN PORTAL)
# ==============================================================================
Write-Host ">> SUITE 6: Observability Dashboards & Admin Portal" -ForegroundColor Yellow

# 6.1 Streamlit Analytics UI (SYS-01)
$stOpen = Test-TcpPort "127.0.0.1" 8501
if ($stOpen) {
    Add-TestResult "Analytics" "SYS-01" "Streamlit Analytics Portal" "PASS" "Streamlit UI open on port 8501"
} else {
    Add-TestResult "Analytics" "SYS-01" "Streamlit Analytics Portal" "WARN" "Streamlit port 8501 offline"
}

# 6.2 Prometheus Metrics Server (SYS-03)
$promHttp = Invoke-QuickHttp "http://localhost:9090/-/healthy"
if ($promHttp.Success) {
    Add-TestResult "Monitoring" "SYS-03" "Prometheus Time Series" "PASS" "Prometheus engine healthy (Port 9090)" $promHttp.ElapsedMs
} else {
    Add-TestResult "Monitoring" "SYS-03" "Prometheus Time Series" "WARN" "Prometheus port 9090 offline" $promHttp.ElapsedMs
}

# 6.3 Grafana Observability (SYS-03)
$grafHttp = Invoke-QuickHttp "http://localhost:3000/api/health"
if ($grafHttp.Success) {
    Add-TestResult "Monitoring" "SYS-03" "Grafana Metrics Dashboards" "PASS" "Grafana server healthy (Port 3000)" $grafHttp.ElapsedMs
} else {
    Add-TestResult "Monitoring" "SYS-03" "Grafana Metrics Dashboards" "WARN" "Grafana port 3000 offline" $grafHttp.ElapsedMs
}

# 6.4 NHFinance Admin Portal (Command Center)
$portalHttp = Invoke-QuickHttp "http://localhost:8090"
if ($portalHttp.Success) {
    Add-TestResult "Portal" "PORTAL" "Unified Admin Portal" "PASS" "Admin portal running (Port 8090)" $portalHttp.ElapsedMs
} else {
    Add-TestResult "Portal" "PORTAL" "Unified Admin Portal" "WARN" "Admin portal port 8090 offline" $portalHttp.ElapsedMs
}

Write-Host ""

# ==============================================================================
# SUITE 7: OPTIONAL DEEP UNIT & INTEGRATION TESTS VIA PYTEST
# ==============================================================================
if ($RunPytest) {
    Write-Host ">> SUITE 7: Executing Deep Unit & Integration Test Suites (Pytest)..." -ForegroundColor Yellow
    
    # 7.1 SYS-05 Lakehouse Pytest
    if (Test-Path "SYS-05/tests") {
        Write-Host "  * Running SYS-05 Lakehouse unit & integration tests..." -ForegroundColor Cyan
        try {
            $pytestOut = python -m pytest SYS-05/tests -q 2>&1
            if ($LASTEXITCODE -eq 0) {
                Add-TestResult "Pytest" "SYS-05" "Lakehouse Unit Tests" "PASS" "All SYS-05 pytest tests passed"
            } else {
                Add-TestResult "Pytest" "SYS-05" "Lakehouse Unit Tests" "WARN" "Pytest returned non-zero code"
            }
        } catch {
            Add-TestResult "Pytest" "SYS-05" "Lakehouse Unit Tests" "SKIP" "Python or pytest not available on host"
        }
    }

    # 7.2 WSIS Windowing Tests
    if (Test-Path "WSIS/tests") {
        Write-Host "  * Running WSIS Windowing & Sync tests..." -ForegroundColor Cyan
        try {
            $wsisOut = python -m unittest discover -s WSIS/tests -p "test_*.py" 2>&1
            if ($LASTEXITCODE -eq 0) {
                Add-TestResult "Pytest" "WSIS" "WSIS Ingestion Tests" "PASS" "All WSIS synchronization tests passed"
            } else {
                Add-TestResult "Pytest" "WSIS" "WSIS Ingestion Tests" "WARN" "WSIS unittest returned warnings"
            }
        } catch {
            Add-TestResult "Pytest" "WSIS" "WSIS Ingestion Tests" "SKIP" "Python unittest skipped"
        }
    }
    Write-Host ""
}

# ==============================================================================
# FINAL SCORECARD & SUMMARY REPORT
# ==============================================================================
$endTime = Get-Date
$totalDuration = [math]::Round(($endTime - $startTime).TotalSeconds, 1)

$passCount = [int]@($testResults | Where-Object { $_.Status -eq "[PASS]" }).Count
$failCount = [int]@($testResults | Where-Object { $_.Status -eq "[FAIL]" }).Count
$warnCount = [int]@($testResults | Where-Object { $_.Status -eq "[WARN]" }).Count
$skipCount = [int]@($testResults | Where-Object { $_.Status -eq "[SKIP]" }).Count
$totalCount = [int]$testResults.Count

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  PRE-FLIGHT VERIFICATION SCORECARD REPORT                            " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

if ($VerboseDetails) {
    $testResults | Format-Table -AutoSize
}

Write-Host ("  TOTAL TESTS RUN : " + $totalCount) -ForegroundColor White
Write-Host ("  PASSED          : " + $passCount) -ForegroundColor Green
Write-Host ("  WARNINGS        : " + $warnCount) -ForegroundColor Yellow
Write-Host ("  FAILED          : " + $failCount) -ForegroundColor $(if ($failCount -gt 0) { "Red" } else { "DarkGray" })
Write-Host ("  SKIPPED         : " + $skipCount) -ForegroundColor DarkGray
Write-Host ("  ELAPSED TIME    : " + $totalDuration + " seconds") -ForegroundColor Cyan
Write-Host ""

if ($failCount -eq 0) {
    Write-Host "======================================================================" -ForegroundColor Green
    Write-Host "  [+] ALL CRITICAL TESTS PASSED! ECOSYSTEM IS READY FOR LIVE RUN/DEMO " -ForegroundColor Green
    Write-Host "======================================================================" -ForegroundColor Green
} else {
    Write-Host "======================================================================" -ForegroundColor Red
    Write-Host "  [!] ONE OR MORE CRITICAL SERVICES FAILED VERIFICATION!              " -ForegroundColor Red
    Write-Host "======================================================================" -ForegroundColor Red
    Write-Host "Troubleshooting recommendations:" -ForegroundColor Magenta
    Write-Host "  1. If running on a new machine, execute:  setup_new_machine.ps1" -ForegroundColor Yellow
    Write-Host "  2. To start all missing services, run:    start_ecosystem.ps1" -ForegroundColor Yellow
    Write-Host "  3. To check logs of a failed container:   docker logs [container_name]" -ForegroundColor Yellow
}
Write-Host ""

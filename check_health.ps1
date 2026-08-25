# ==============================================================================
# ENTERPRISE DATA & AI ECOSYSTEM - PORT & HEALTH CHECKER
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  QUET TRANG THAI HOAT DONG TOAN BO CAY DICH VU (HEALTH CHECK)        " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

$services = @(
    @{ System = "SYS-01"; Name = "Streamlit Analytics Dashboard"; Port = 8501; Type = "Web UI" },
    @{ System = "SYS-02"; Name = "Airflow Webserver";             Port = 8085; Type = "Web UI" },
    @{ System = "SYS-02"; Name = "Airflow Metadata Postgres";     Port = 5433; Type = "Database" },
    @{ System = "SYS-03"; Name = "Grafana Monitoring Dashboard";  Port = 3000; Type = "Web UI" },
    @{ System = "SYS-03"; Name = "Prometheus Metrics Server";     Port = 9090; Type = "Metrics" },
    @{ System = "SYS-04"; Name = "FastAPI Model Inference REST";  Port = 8000; Type = "REST API" },
    @{ System = "SYS-04"; Name = "MLflow Model Tracking UI";      Port = 5000; Type = "Web UI" },
    @{ System = "SYS-05"; Name = "MinIO S3 API";                  Port = 9000; Type = "S3 Storage" },
    @{ System = "SYS-05"; Name = "MinIO Web Console";             Port = 9001; Type = "Web UI" },
    @{ System = "SYS-05"; Name = "ClickHouse OLAP HTTP";          Port = 8123; Type = "Database" },
    @{ System = "SYS-05"; Name = "ClickHouse Native TCP";         Port = 9004; Type = "Database" },
    @{ System = "SYS-05"; Name = "Enterprise Metadata Postgres";  Port = 5432; Type = "Database" }
)

$results = @()

foreach ($svc in $services) {
    $port = $svc.Port
    $isOpen = $false
    
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $connect = $tcp.BeginConnect("127.0.0.1", $port, $null, $null)
        $wait = $connect.AsyncWaitHandle.WaitOne(400, $false)
        if ($wait) {
            $tcp.EndConnect($connect)
            $isOpen = $true
        }
        $tcp.Close()
    } catch {
        $isOpen = $false
    }

    if ($isOpen) {
        $statusStr = "[ONLINE]"
    } else {
        $statusStr = "[OFFLINE]"
    }
    
    $results += [PSCustomObject]@{
        "He Thong"   = $svc.System
        "Dich Vu"    = $svc.Name
        "Cong Port"  = $svc.Port
        "Loai"       = $svc.Type
        "Trang Thai" = $statusStr
    }
}

$results | Format-Table -AutoSize

Write-Host ""
Write-Host "-> De khoi dong cac dich vu con tat, chay: .\start_ecosystem.ps1" -ForegroundColor Yellow
Write-Host "-> De mo giao dien tong quan, mo file:     .\index.html" -ForegroundColor Cyan
Write-Host ""

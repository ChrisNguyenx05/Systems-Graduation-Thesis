# ==============================================================================
# 🌐 PUBLIC DEMO TUNNEL - CLOUDFLARE / LOCALTUNNEL / NGROK
# ==============================================================================

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  🌐 KHỞI ĐỘNG ĐƯỜNG HẦM DEMO CÔNG KHAI (PUBLIC HTTPS TUNNEL 0Đ)     " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Chọn dịch vụ bạn muốn public đường hầm HTTPS ra ngoài Internet để gửi link cho Hội đồng chấm thi:" -ForegroundColor Yellow
Write-Host "  [1] SYS-02: Apache Airflow (Port 8085)" -ForegroundColor White
Write-Host "  [2] SYS-05: MinIO S3 Console (Port 9001)" -ForegroundColor White
Write-Host "  [3] SYS-01: Streamlit Dashboard (Port 8501)" -ForegroundColor White
Write-Host "  [4] SYS-04: FastAPI Swagger Docs (Port 8000)" -ForegroundColor White
Write-Host "  [5] SYS-03: Grafana Monitoring (Port 3000)" -ForegroundColor White
Write-Host "  [6] Tuỳ chọn Port khác" -ForegroundColor White
Write-Host ""

$choice = Read-Host "Nhập lựa chọn của bạn (1-6) [Mặc định: 1]"
if ([string]::IsNullOrWhiteSpace($choice)) { $choice = "1" }

$targetPort = 8085
$serviceName = "SYS-02 Airflow"

switch ($choice) {
    "1" { $targetPort = 8085; $serviceName = "SYS-02 Airflow" }
    "2" { $targetPort = 9001; $serviceName = "SYS-05 MinIO Console" }
    "3" { $targetPort = 8501; $serviceName = "SYS-01 Streamlit Dashboard" }
    "4" { $targetPort = 8000; $serviceName = "SYS-04 FastAPI" }
    "5" { $targetPort = 3000; $serviceName = "SYS-03 Grafana" }
    "6" { 
        $custom = Read-Host "Nhập số cổng Port cần mở"
        $targetPort = [int]$custom
        $serviceName = "Custom Port $targetPort"
    }
    Default { $targetPort = 8085; $serviceName = "SYS-02 Airflow" }
}

Write-Host ""
Write-Host "🚀 Đang chuẩn bị đường hầm HTTPS cho $serviceName (Port $targetPort)..." -ForegroundColor Green

# 1. Thử dùng Cloudflare Tunnel (Miễn phí 100%, không cần đăng ký tài khoản)
$hasCloudflared = Get-Command cloudflared -ErrorAction SilentlyContinue
if ($hasCloudflared) {
    Write-Host "⚡ Sử dụng Cloudflare Tunnel (Miễn phí & Bảo mật)..." -ForegroundColor Cyan
    Write-Host "👉 Đường link public https://...trycloudflare.com sẽ xuất hiện bên dưới:" -ForegroundColor Yellow
    cloudflared tunnel --url "http://localhost:$targetPort"
    exit 0
}

# 2. Thử dùng npx localtunnel (Node.js)
$hasNpx = Get-Command npx -ErrorAction SilentlyContinue
if ($hasNpx) {
    Write-Host "⚡ Sử dụng Localtunnel qua NPX (Node.js)..." -ForegroundColor Cyan
    Write-Host "👉 Đường link public https://...loca.lt sẽ xuất hiện bên dưới:" -ForegroundColor Yellow
    npx localtunnel --port $targetPort
    exit 0
}

# 3. Thử dùng Ngrok
$hasNgrok = Get-Command ngrok -ErrorAction SilentlyContinue
if ($hasNgrok) {
    Write-Host "⚡ Sử dụng Ngrok..." -ForegroundColor Cyan
    ngrok http $targetPort
    exit 0
}

# Nếu chưa cài công cụ nào
Write-Host "⚠️ Chưa phát hiện công cụ Tunnel (cloudflared, localtunnel hoặc ngrok) trong máy." -ForegroundColor Yellow
Write-Host ""
Write-Host "💡 HƯỚNG DẪN CÀI ĐẶT NHANH 1 TRONG CÁC CÔNG CỤ SAU (CHỈ CẦN 1 CÔNG CỤ):" -ForegroundColor Magenta
Write-Host "  Cách 1 (Khuyên dùng): Tải cloudflared.exe từ https://github.com/cloudflare/cloudflared/releases" -ForegroundColor White
Write-Host "                        hoặc chạy lệnh: winget install Cloudflare.cloudflared" -ForegroundColor White
Write-Host "  Cách 2: Cài Node.js rồi chạy lại script này (sẽ tự động dùng npx localtunnel)" -ForegroundColor White
Write-Host "  Cách 3: Tải ngrok từ https://ngrok.com/download" -ForegroundColor White
Write-Host ""

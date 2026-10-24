# ==============================================================================
# START TUNNEL IN BACKGROUND (RUNS IN BACKGROUND WITHOUT BLOCKING SHELL)
# ==============================================================================

$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
$logPath = Join-Path $PSScriptRoot "tunnel.log"
$urlFilePath = Join-Path $PSScriptRoot "TUNNEL_URL.txt"

# 1. Terminate previous tunnel processes if any
Get-Process cloudflared -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
if (Test-Path $logPath) { Remove-Item $logPath -Force -ErrorAction SilentlyContinue }

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  STARTING CLOUDFLARE BACKGROUND TUNNEL FOR ADMIN PORTAL (PORT 8090)  " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Find cloudflared executable
$cfBin = $null
if (Get-Command cloudflared -ErrorAction SilentlyContinue) {
    $cfBin = "cloudflared"
} elseif (Test-Path (Join-Path $PSScriptRoot "cloudflared.exe")) {
    $cfBin = Join-Path $PSScriptRoot "cloudflared.exe"
} else {
    Write-Host "❌ ERROR: 'cloudflared' command was not found on this system!" -ForegroundColor Red
    Write-Host "👉 Install it using: winget install Cloudflare.cloudflared" -ForegroundColor Yellow
    Write-Host "   or place cloudflared.exe in this repository folder." -ForegroundColor Yellow
    exit 1
}

Write-Host "Connecting to Cloudflare Edge Network..." -ForegroundColor Yellow

# 2. Launch cloudflared hidden in background
$proc = Start-Process -FilePath $cfBin -ArgumentList "tunnel --url http://localhost:8090" -RedirectStandardError $logPath -WindowStyle Hidden -PassThru

# 3. Poll log file to extract the public trycloudflare.com URL
$tunnelUrl = ""
for ($i = 0; $i -lt 25; $i++) {
    Start-Sleep -Milliseconds 800
    if (Test-Path $logPath) {
        $content = Get-Content $logPath -Raw -ErrorAction SilentlyContinue
        if ($content -match "(https://[a-zA-Z0-9-]+\.trycloudflare\.com)") {
            $tunnelUrl = $matches[1]
            break
        }
    }
}

if ($tunnelUrl) {
    Set-Content -Path $urlFilePath -Value $tunnelUrl -Force
    Set-Clipboard -Value $tunnelUrl -ErrorAction SilentlyContinue

    Write-Host ""
    Write-Host "🎉 PUBLIC HTTPS TUNNEL ESTABLISHED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host ""
    Write-Host "👉 ADMIN PORTAL URL: " -NoNewline -ForegroundColor Cyan
    Write-Host "$tunnelUrl" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "✅ Link copied to Clipboard (Press Ctrl+V in your browser)." -ForegroundColor Green
    Write-Host "✅ Link saved to: TUNNEL_URL.txt" -ForegroundColor Green
    Write-Host ""
    Write-Host "💡 YOU MAY SAFELY CLOSE THIS POWERSHELL WINDOW NOW!" -ForegroundColor Magenta
    Write-Host "   The tunnel will continue running quietly in the background." -ForegroundColor White
    Write-Host "   (To stop the tunnel later, run: .\stop_tunnel.ps1)" -ForegroundColor DarkGray
    Write-Host ""
} else {
    Write-Host "⚠️ Warning: Could not retrieve Tunnel URL within the expected wait time." -ForegroundColor Red
    Write-Host "Check diagnostic details at: $logPath" -ForegroundColor DarkGray
}

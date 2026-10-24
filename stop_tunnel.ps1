# ==============================================================================
# STOP CLOUDFLARE TUNNEL (CLEANUP BACKGROUND PROCESSES)
# ==============================================================================

Write-Host ""
Write-Host "Stopping Cloudflare Tunnel background processes..." -ForegroundColor Yellow

$procs = Get-Process cloudflared -ErrorAction SilentlyContinue
if ($procs) {
    $procs | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "✅ Cloudflare Tunnel stopped successfully." -ForegroundColor Green
} else {
    Write-Host "ℹ️ No running cloudflared process found." -ForegroundColor DarkGray
}

$urlFilePath = Join-Path $PSScriptRoot "TUNNEL_URL.txt"
if (Test-Path $urlFilePath) {
    Remove-Item $urlFilePath -Force -ErrorAction SilentlyContinue
}
Write-Host ""

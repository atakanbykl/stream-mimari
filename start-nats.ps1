# Stream Mimari NATS Server Baslatma Scripti
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Stream Mimari NATS Server Baslatiliyor..." -ForegroundColor Green
Write-Host "  Konfigurasyon: nats-server.conf" -ForegroundColor White
Write-Host "  Client Port  : 4222" -ForegroundColor White
Write-Host "  Monitoring   : http://localhost:8222" -ForegroundColor Yellow
Write-Host "========================================================" -ForegroundColor Cyan

nats-server -c "$PSScriptRoot\nats-server.conf"

#Requires -Version 5.1
# stop.ps1 — Para todos los contenedores de SentimentFlow sin borrar datos

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

Write-Host ""
Write-Host "==> Parando SentimentFlow..." -ForegroundColor Cyan
docker compose stop
Write-Host "  [OK] Todos los contenedores detenidos. Los datos se conservan." -ForegroundColor Green
Write-Host "       Para volver a arrancar: .\start.ps1  (o  docker compose up -d)" -ForegroundColor Gray
Write-Host ""

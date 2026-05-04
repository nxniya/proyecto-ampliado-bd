#Requires -Version 5.1
# reset.ps1 — Para y elimina TODOS los datos de SentimentFlow (volúmenes + contenedores)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

Write-Host ""
Write-Host "  ADVERTENCIA: Esto eliminará todos los datos (PostgreSQL, MinIO, RabbitMQ, logs de Airflow)." -ForegroundColor Yellow
$confirm = Read-Host "  ¿Estás seguro? Escribe 'si' para confirmar"

if ($confirm -ne "si") {
    Write-Host "  Operación cancelada."
    exit 0
}

Write-Host ""
Write-Host "==> Eliminando contenedores y volúmenes..." -ForegroundColor Cyan
docker compose down -v --remove-orphans
Write-Host "  [OK] Reset completo. La próxima ejecución de start.ps1 comenzará desde cero." -ForegroundColor Green
Write-Host ""

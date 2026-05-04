#Requires -Version 5.1
<#
.SYNOPSIS
    Arranca el pipeline completo de SentimentFlow.
.DESCRIPTION
    1. Verifica requisitos (Docker, puertos libres)
    2. Crea .env si no existe
    3. Lanza docker compose up --build -d
    4. Espera a que todos los servicios esten sanos
    5. Abre los navegadores con los servicios principales
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ─── Colores ──────────────────────────────────────────────────────────────────
function Write-Header  { param($msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Write-Ok      { param($msg) Write-Host "  [OK] $msg" -ForegroundColor Green }
function Write-Warn    { param($msg) Write-Host "  [!]  $msg" -ForegroundColor Yellow }
function Write-Fail    { param($msg) Write-Host "  [X]  $msg" -ForegroundColor Red }
function Write-Info    { param($msg) Write-Host "       $msg" -ForegroundColor Gray }

# ─── Directorio raiz del proyecto ──────────────────────────────────────────────
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

Write-Host ""
Write-Host "  SentimentFlow -- Script de arranque" -ForegroundColor Magenta
Write-Host "  ====================================" -ForegroundColor Magenta
Write-Host ""

# ─── 1. Verificar Docker ──────────────────────────────────────────────────────
Write-Header "Verificando requisitos"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Fail "Docker no esta instalado o no esta en el PATH."
    Write-Info "Descarga Docker Desktop desde https://www.docker.com/products/docker-desktop"
    exit 1
}
$dockerVersion = docker --version 2>&1
Write-Ok "Docker: $dockerVersion"

docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Docker daemon no esta corriendo. Inicia Docker Desktop y vuelve a intentarlo."
    exit 1
}
Write-Ok "Docker daemon activo."

docker compose version 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Fail "docker compose (v2) no esta disponible. Actualiza Docker Desktop."
    exit 1
}
$composeVersion = docker compose version 2>&1
Write-Ok "Docker Compose: $composeVersion"

# ─── 2. Verificar puertos libres ──────────────────────────────────────────────
Write-Header "Comprobando puertos"

$RequiredPorts = @{
    5432  = "PostgreSQL"
    5672  = "RabbitMQ AMQP"
    8080  = "Airflow UI"
    8501  = "Streamlit Dashboard"
    8889  = "JupyterLab"
    9000  = "MinIO S3 API"
    9001  = "MinIO Console"
    15672 = "RabbitMQ Management"
}

$PortConflicts = @()
foreach ($port in $RequiredPorts.Keys) {
    $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    if ($conn) {
        $ownerPid = ($conn | Select-Object -First 1).OwningProcess
        $proc = Get-Process -Id $ownerPid -ErrorAction SilentlyContinue
        $procName = if ($proc) { $proc.Name } else { "PID $ownerPid" }
        Write-Warn "Puerto $port ($($RequiredPorts[$port])) ocupado por '$procName'."
        $PortConflicts += $port
    } else {
        Write-Ok "Puerto $port libre  ($($RequiredPorts[$port]))"
    }
}

if ($PortConflicts.Count -gt 0) {
    Write-Host ""
    Write-Warn "Hay $($PortConflicts.Count) puerto(s) en conflicto. El arranque puede fallar."
    $resp = Read-Host "  Continuar de todas formas? [s/N]"
    if ($resp -notmatch '^[sS]$') { exit 1 }
}

# ─── 3. Crear .env si no existe ───────────────────────────────────────────────
Write-Header "Configurando variables de entorno"

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Ok "Creado .env desde .env.example (valores por defecto para desarrollo local)."
} else {
    Write-Ok ".env ya existe."
}

# ─── 4. Construir e iniciar contenedores ──────────────────────────────────────
Write-Header "Construyendo e iniciando contenedores"
Write-Info "El primer arranque puede tardar 5-15 minutos (descarga de imagenes base)."
Write-Info "Los arranques posteriores seran inmediatos."
Write-Host ""

docker compose up -d --build
if ($LASTEXITCODE -ne 0) {
    Write-Fail "docker compose up fallo. Revisa los errores anteriores."
    exit 1
}

# ─── 5. Esperar servicios criticos ─────────────────────────────────────────────
Write-Header "Esperando a que los servicios esten sanos"

# Servicios a monitorizar y su tiempo maximo de espera (segundos)
$Services = [ordered]@{
    "rabbitmq"           = 60
    "minio"              = 60
    "postgres"           = 60
    "airflow_webserver"  = 240
    "review_consumer"    = 300
    "sentiment_dashboard"= 60
}

foreach ($svc in $Services.Keys) {
    $maxWait   = $Services[$svc]
    $elapsed   = 0
    $interval  = 5
    $ready     = $false

    Write-Host "  Esperando '$svc'..." -NoNewline

    while ($elapsed -lt $maxWait) {
        $status = docker inspect --format='{{.State.Health.Status}}' $svc 2>$null
        if (-not $status) {
            # Contenedor sin healthcheck: verificar que este running
            $running = docker inspect --format='{{.State.Running}}' $svc 2>$null
            if ($running -eq "true") { $ready = $true; break }
        } elseif ($status -eq "healthy") {
            $ready = $true; break
        } elseif ($status -eq "exited") {
            # Contenedores init: comprobar exit code 0
            $exitCode = docker inspect --format='{{.State.ExitCode}}' $svc 2>$null
            if ($exitCode -eq "0") { $ready = $true; break }
            Write-Host ""
            Write-Fail "'$svc' termino con error (exit code $exitCode)."
            Write-Info "Ejecuta: docker compose logs $svc"
            break
        }
        Start-Sleep -Seconds $interval
        $elapsed += $interval
        Write-Host "." -NoNewline
    }

    if ($ready) {
        Write-Host " listo." -ForegroundColor Green
    } elseif (-not ($status -eq "exited" -and $exitCode -ne "0")) {
        Write-Host ""
        Write-Warn "'$svc' no esta sano tras $maxWait s. Puede necesitar mas tiempo."
        Write-Info "Ejecuta: docker compose ps"
    }
}

# ─── 6. Resumen final ─────────────────────────────────────────────────────────
Write-Host ""
Write-Host "-----------------------------------------------------" -ForegroundColor DarkGray
Write-Host "  SentimentFlow esta en marcha" -ForegroundColor Green
Write-Host "-----------------------------------------------------" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Servicio               URL                          Credenciales" -ForegroundColor White
Write-Host "  -----------------------------------------------------------------" -ForegroundColor DarkGray
Write-Host "  Dashboard Streamlit    http://localhost:8501        --" -ForegroundColor Cyan
Write-Host "  Airflow UI             http://localhost:8080        airflow / airflow" -ForegroundColor Cyan
Write-Host "  RabbitMQ Management    http://localhost:15672       admin / admin123" -ForegroundColor Cyan
Write-Host "  MinIO Console          http://localhost:9001        minioadmin / minioadmin" -ForegroundColor Cyan
Write-Host "  JupyterLab             http://localhost:8888        (sin contrasena)" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Comandos utiles:" -ForegroundColor White
Write-Host "    docker compose ps                         -- estado de contenedores" -ForegroundColor Gray
Write-Host "    docker compose logs -f producer consumer -- logs en tiempo real" -ForegroundColor Gray
Write-Host "    .\stop.ps1                                -- parar sin borrar datos" -ForegroundColor Gray
Write-Host "    .\reset.ps1                               -- parar y borrar todos los datos" -ForegroundColor Gray
Write-Host ""

# ─── 7. Abrir navegadores (opcional) ─────────────────────────────────────────
$openBrowser = Read-Host "  Abrir los servicios en el navegador? [S/n]"
if ($openBrowser -notmatch '^[nN]$') {
    Start-Process "http://localhost:8501"
    Start-Sleep -Milliseconds 500
    Start-Process "http://localhost:8080"
    Start-Sleep -Milliseconds 500
    Start-Process "http://localhost:15672"
    Write-Ok "Navegadores abiertos."
}

Write-Host ""

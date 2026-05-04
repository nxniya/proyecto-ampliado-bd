#!/usr/bin/env bash
# start.sh — Arranca el pipeline completo de SentimentFlow
# Uso: ./start.sh
set -euo pipefail

# ─── Colores ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; MAGENTA='\033[0;35m'; GRAY='\033[0;37m'; NC='\033[0m'

ok()   { echo -e "  ${GREEN}[OK]${NC} $*"; }
warn() { echo -e "  ${YELLOW}[!] ${NC} $*"; }
fail() { echo -e "  ${RED}[X] ${NC} $*"; }
info() { echo -e "       ${GRAY}$*${NC}"; }
header(){ echo -e "\n${CYAN}==> $*${NC}"; }

# ─── Directorio raíz ──────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo -e "${MAGENTA}  SentimentFlow — Script de arranque${NC}"
echo -e "${MAGENTA}  ====================================${NC}"
echo ""

# ─── 1. Verificar Docker ──────────────────────────────────────────────────────
header "Verificando requisitos"

if ! command -v docker &>/dev/null; then
    fail "Docker no está instalado."
    info "Instálalo desde https://docs.docker.com/get-docker/"
    exit 1
fi
ok "Docker: $(docker --version)"

if ! docker info &>/dev/null; then
    fail "Docker daemon no está corriendo. Inicia Docker Desktop o el servicio docker."
    exit 1
fi
ok "Docker daemon activo."

if ! docker compose version &>/dev/null; then
    fail "docker compose (v2) no está disponible. Actualiza Docker."
    exit 1
fi
ok "Docker Compose: $(docker compose version)"

# ─── 2. Verificar puertos libres ──────────────────────────────────────────────
header "Comprobando puertos"

declare -A PORTS=(
    [5432]="PostgreSQL"
    [5672]="RabbitMQ AMQP"
    [8080]="Airflow UI"
    [8501]="Streamlit Dashboard"
    [8888]="JupyterLab"
    [9000]="MinIO S3 API"
    [9001]="MinIO Console"
    [15672]="RabbitMQ Management"
)

CONFLICTS=0
for port in "${!PORTS[@]}"; do
    if ss -tlnp 2>/dev/null | grep -q ":$port " || \
       lsof -i ":$port" &>/dev/null 2>&1; then
        warn "Puerto $port (${PORTS[$port]}) está ocupado."
        CONFLICTS=$((CONFLICTS + 1))
    else
        ok "Puerto $port libre  (${PORTS[$port]})"
    fi
done

if [ "$CONFLICTS" -gt 0 ]; then
    echo ""
    warn "$CONFLICTS puerto(s) en conflicto. El arranque puede fallar."
    read -r -p "  ¿Continuar de todas formas? [s/N] " resp
    [[ "$resp" =~ ^[sS]$ ]] || exit 1
fi

# ─── 3. Crear .env si no existe ───────────────────────────────────────────────
header "Configurando variables de entorno"

if [ ! -f ".env" ]; then
    cp .env.example .env
    ok "Creado .env desde .env.example (valores por defecto para desarrollo local)."
else
    ok ".env ya existe."
fi

# ─── 4. Construir e iniciar contenedores ──────────────────────────────────────
header "Construyendo e iniciando contenedores"
info "El primer arranque puede tardar 5-15 minutos (descarga de imágenes base)."
info "Los arranques posteriores serán inmediatos."
echo ""

docker compose up -d --build

# ─── 5. Esperar servicios críticos ────────────────────────────────────────────
header "Esperando a que los servicios estén sanos"

wait_for_service() {
    local svc="$1"
    local max_wait="$2"
    local interval=5
    local elapsed=0
    local ready=false

    printf "  Esperando '%s'..." "$svc"

    while [ "$elapsed" -lt "$max_wait" ]; do
        local status
        status=$(docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "")

        if [ -z "$status" ]; then
            # Sin healthcheck: comprobar que esté running
            local running
            running=$(docker inspect --format='{{.State.Running}}' "$svc" 2>/dev/null || echo "false")
            if [ "$running" = "true" ]; then ready=true; break; fi
        elif [ "$status" = "healthy" ]; then
            ready=true; break
        elif [ "$status" = "exited" ]; then
            local exit_code
            exit_code=$(docker inspect --format='{{.State.ExitCode}}' "$svc" 2>/dev/null || echo "1")
            if [ "$exit_code" = "0" ]; then ready=true; break; fi
            echo ""
            fail "'$svc' terminó con error (exit code $exit_code)."
            info "Ejecuta: docker compose logs $svc"
            return 1
        fi
        sleep "$interval"
        elapsed=$((elapsed + interval))
        printf "."
    done

    if $ready; then
        echo -e " ${GREEN}listo.${NC}"
    else
        echo ""
        warn "'$svc' no está sano tras ${max_wait}s. Puede necesitar más tiempo."
        info "Ejecuta: docker compose ps"
    fi
}

wait_for_service rabbitmq            60
wait_for_service minio               60
wait_for_service postgres            60
wait_for_service airflow_webserver  240
wait_for_service review_consumer    300
wait_for_service sentiment_dashboard 60

# ─── 6. Resumen final ─────────────────────────────────────────────────────────
echo ""
echo -e "${GRAY}─────────────────────────────────────────────────────${NC}"
echo -e "${GREEN}  SentimentFlow está en marcha${NC}"
echo -e "${GRAY}─────────────────────────────────────────────────────${NC}"
echo ""
echo -e "  ${CYAN}Servicio               URL                          Credenciales${NC}"
echo -e "  ${GRAY}─────────────────────────────────────────────────────────────────${NC}"
echo -e "  Dashboard Streamlit    http://localhost:8501        —"
echo -e "  Airflow UI             http://localhost:8080        airflow / airflow"
echo -e "  RabbitMQ Management    http://localhost:15672       admin / admin123"
echo -e "  MinIO Console          http://localhost:9001        minioadmin / minioadmin"
echo -e "  JupyterLab             http://localhost:8888        (sin contraseña)"
echo ""
echo -e "  ${GRAY}Comandos útiles:${NC}"
echo -e "  ${GRAY}  docker compose ps                        — estado de contenedores${NC}"
echo -e "  ${GRAY}  docker compose logs -f producer consumer — logs en tiempo real${NC}"
echo -e "  ${GRAY}  ./stop.sh                                — parar sin borrar datos${NC}"
echo -e "  ${GRAY}  ./reset.sh                               — parar y borrar todos los datos${NC}"
echo ""

# ─── 7. Abrir navegadores (si hay entorno gráfico) ───────────────────────────
if [ -n "${DISPLAY:-}" ] || [ "$(uname)" = "Darwin" ]; then
    read -r -p "  ¿Abrir los servicios en el navegador? [S/n] " open_browser
    if [[ ! "$open_browser" =~ ^[nN]$ ]]; then
        if [ "$(uname)" = "Darwin" ]; then
            open "http://localhost:8501" &
            open "http://localhost:8080" &
            open "http://localhost:15672" &
        else
            xdg-open "http://localhost:8501" &>/dev/null &
            xdg-open "http://localhost:8080" &>/dev/null &
            xdg-open "http://localhost:15672" &>/dev/null &
        fi
        ok "Navegadores abiertos."
    fi
fi

echo ""

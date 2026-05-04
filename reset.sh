#!/usr/bin/env bash
# reset.sh — Para y elimina TODOS los datos de SentimentFlow (volúmenes + contenedores)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo -e "\033[1;33m  ADVERTENCIA: Esto eliminará todos los datos (PostgreSQL, MinIO, RabbitMQ, logs de Airflow).\033[0m"
read -r -p "  ¿Estás seguro? Escribe 'si' para confirmar: " confirm

if [ "$confirm" != "si" ]; then
    echo "  Operación cancelada."
    exit 0
fi

echo ""
echo -e "\033[0;36m==> Eliminando contenedores y volúmenes...\033[0m"
docker compose down -v --remove-orphans
echo -e "\033[0;32m  [OK] Reset completo. La próxima ejecución de start.sh comenzará desde cero.\033[0m"
echo ""

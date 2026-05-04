#!/usr/bin/env bash
# stop.sh — Para todos los contenedores de SentimentFlow sin borrar datos
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo -e "\033[0;36m==> Parando SentimentFlow...\033[0m"
docker compose stop
echo -e "\033[0;32m  [OK] Todos los contenedores detenidos. Los datos se conservan.\033[0m"
echo -e "\033[0;37m       Para volver a arrancar: ./start.sh  (o  docker compose up -d)\033[0m"
echo ""

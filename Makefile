# Makefile — SentimentFlow
# Uso: make <target>
# Disponible en Linux y Mac. En Windows usar los scripts .ps1.

.PHONY: help start stop reset logs status ps db-query

# Objetivo por defecto
help:
	@echo ""
	@echo "  SentimentFlow — Comandos disponibles"
	@echo "  ======================================"
	@echo ""
	@echo "  make start        Arrancar todo el pipeline (build + up)"
	@echo "  make stop         Parar contenedores (conserva datos)"
	@echo "  make reset        Parar y eliminar todos los datos (volúmenes)"
	@echo "  make logs         Ver logs en tiempo real (todos los servicios)"
	@echo "  make logs-pipe    Ver logs del producer y consumer"
	@echo "  make status       Estado resumido de los contenedores"
	@echo "  make ps           Estado completo (docker compose ps)"
	@echo "  make db-query     Top 10 productos por reseñas en PostgreSQL"
	@echo "  make open         Abrir servicios en el navegador"
	@echo ""

start:
	@chmod +x start.sh
	@./start.sh

stop:
	@chmod +x stop.sh
	@./stop.sh

reset:
	@chmod +x reset.sh
	@./reset.sh

logs:
	docker compose logs -f

logs-pipe:
	docker compose logs -f producer consumer

status:
	@echo ""
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
	@echo ""

ps:
	docker compose ps

db-query:
	@docker exec -it postgres psql -U postgres -d reviewsdb -c \
	"SELECT product_name, positive, neutral, negative, total_reviews \
	 FROM product_stats ORDER BY total_reviews DESC LIMIT 10;"

open:
	@if [ "$$(uname)" = "Darwin" ]; then \
	    open http://localhost:8501 & \
	    open http://localhost:8080 & \
	    open http://localhost:15672; \
	else \
	    xdg-open http://localhost:8501 & \
	    xdg-open http://localhost:8080 & \
	    xdg-open http://localhost:15672; \
	fi

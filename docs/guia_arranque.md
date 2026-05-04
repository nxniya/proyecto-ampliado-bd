# Guía de Arranque — SentimentFlow

Guía paso a paso para poner en marcha el pipeline completo desde cero.

---

## Requisitos previos

| Herramienta | Versión mínima | Verificación |
|---|---|---|
| Docker Desktop | 4.x (o Docker Engine ≥ 24) | `docker --version` |
| Docker Compose | v2 integrado en Docker | `docker compose version` |
| RAM disponible | ≥ 4 GB para los contenedores | — |
| Puertos libres | 5432, 5672, 8080, 8501, 8888, 9000, 9001, 15672 | — |

> **Windows**: usa Docker Desktop con backend WSL 2.  
> **Linux/Mac**: Docker Engine + plugin `docker-compose-plugin` (v2).

---

## Inicio rápido (automatizado)

### Windows (PowerShell)

```powershell
# Desde la raíz del proyecto
.\start.ps1
```

### Linux / Mac (Bash)

```bash
# Dar permisos la primera vez
chmod +x start.sh stop.sh reset.sh

./start.sh
```

Ambos scripts realizan automáticamente todos los pasos descritos a continuación.

---

## Inicio manual paso a paso

### Paso 1 — Clonar / actualizar el repositorio

```bash
git clone <url-del-repo>
cd proyecto-ampliado

# O si ya está clonado:
git pull
```

### Paso 2 — Crear el fichero de variables de entorno

```bash
# Linux / Mac
cp .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env
```

Los valores por defecto de `.env.example` funcionan para desarrollo local sin ningún cambio.

### Paso 3 — Construir y levantar los contenedores

```bash
docker compose up -d --build
```

> El primer arranque tarda **5-15 minutos** porque descarga imágenes base y construye
> las imágenes de Airflow (Java + PySpark) y Jupyter (PySpark). Los arranques
> posteriores son inmediatos (`docker compose up -d`).

### Paso 4 — Esperar a que los servicios estén sanos

```bash
docker compose ps
```

Espera hasta que todos los servicios muestren `healthy` o `running`:

| Contenedor | Estado esperado |
|---|---|
| `rabbitmq` | `healthy` |
| `minio` | `healthy` |
| `minio_init` | `exited (0)` |
| `postgres` | `healthy` |
| `airflow_init` | `exited (0)` |
| `airflow_webserver` | `healthy` |
| `airflow_scheduler` | `running` |
| `jupyter_spark` | `running` |
| `review_producer` | `running` |
| `review_consumer` | `running` |
| `sentiment_dashboard` | `running` |

> Si algún servicio aparece en `starting`, espera 30 segundos y vuelve a ejecutar
> `docker compose ps`. Los primeros en estar listos son `rabbitmq`, `minio` y
> `postgres`; los últimos son `airflow_webserver` y `review_consumer`.

### Paso 5 — Verificar que el pipeline está activo

Comprueba que llegan mensajes a RabbitMQ:

```
http://localhost:15672  →  admin / admin123
→ Queues → reviews_queue → debe mostrar mensajes entrantes
```

Comprueba que el consumer sube ficheros a MinIO:

```
http://localhost:9001  →  minioadmin / minioadmin
→ Buckets → raw-reviews → deben aparecer ficheros .jsonl
```

### Paso 6 — Acceder a los servicios

| Servicio | URL | Credenciales |
|---|---|---|
| **Dashboard Streamlit** | http://localhost:8501 | — |
| **Airflow UI** | http://localhost:8080 | `airflow` / `airflow` |
| **RabbitMQ Management** | http://localhost:15672 | `admin` / `admin123` |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **JupyterLab** | http://localhost:8888 | sin contraseña |
| **PostgreSQL** | `localhost:5432` | `postgres` / `postgres` / db: `reviewsdb` |

---

## Flujo de datos en producción

```
[Producer ~3 msg/s]
       │  AMQP
       ▼
  [RabbitMQ]
       │  consume
       ▼
  [Consumer]  ── 100 msgs o 30 s ──►  MinIO/raw-reviews/*.jsonl
       │
       │  REST trigger
       ▼
  [Airflow DAG: reviews_etl]
       │
       ├─ Task 1: sentiment_analysis.py (PySpark + VADER)
       │         MinIO/raw-reviews → MinIO/processed-reviews/*.parquet
       │
       └─ Task 2: aggregations.py (PySpark)
                 MinIO/processed-reviews → PostgreSQL (tablas: reviews, product_stats)
                              │
                              ▼
                   [Dashboard Streamlit :8501]
                   (se refresca automáticamente cada 30 s)
```

---

## Comandos útiles durante la demostración

### Ver logs en tiempo real

```bash
# Todos los servicios
docker compose logs -f

# Solo producer y consumer
docker compose logs -f producer consumer

# Solo Airflow
docker compose logs -f airflow-webserver airflow-scheduler
```

### Ver el estado de las ejecuciones de Airflow

```
http://localhost:8080  →  DAGs → reviews_etl → Graph / Calendar
```

### Consultar la base de datos directamente

```bash
docker exec -it postgres psql -U postgres -d reviewsdb -c "
  SELECT product_name, positive, neutral, negative, total_reviews
  FROM product_stats
  ORDER BY total_reviews DESC
  LIMIT 10;
"
```

### Cambiar la velocidad del producer en caliente

```bash
# Subir a 10 mensajes/segundo
docker compose stop producer
EVENTS_PER_SECOND=10 docker compose up -d producer
```

---

## Parar y reiniciar

### Parar sin borrar datos

```bash
# Linux / Mac
./stop.sh

# Windows
.\stop.ps1
```

O manualmente: `docker compose stop`

### Parar y borrar todos los datos (reset completo)

```bash
# Linux / Mac
./reset.sh

# Windows
.\reset.ps1
```

O manualmente: `docker compose down -v --remove-orphans`

---

## Resolución de problemas frecuentes

### `airflow_webserver` tarda en estar `healthy`

Normal. Airflow necesita ~2-3 minutos en el primer arranque para migrar la base de datos. Espera y ejecuta de nuevo `docker compose ps`.

### `review_consumer` aparece en `restarting`

El consumer espera a que `airflow_webserver` esté `healthy` antes de arrancar. Si Airflow aún no está listo, Docker reiniciará el consumer automáticamente hasta que lo esté.

### Puerto ya en uso

```bash
# Ver qué proceso usa el puerto (ejemplo: 8080)
# Windows
netstat -ano | findstr :8080

# Linux / Mac
lsof -i :8080
```

Modifica el puerto en `docker-compose.yml` (lado izquierdo del mapping, e.g. `"8081:8080"`).

### No aparecen datos en el dashboard

1. Comprueba que `review_producer` y `review_consumer` están `running`.
2. Revisa en MinIO que existen ficheros en `raw-reviews/`.
3. Comprueba en Airflow que el DAG `reviews_etl` no está en pausa (toggle verde en la UI).
4. Espera al menos 30-60 segundos para el primer batch completo.

### Error de memoria en PySpark

Airflow+PySpark es el componente más pesado. Si Docker Desktop tiene menos de 4 GB asignados:
- **Windows**: Docker Desktop → Settings → Resources → Memory → aumentar a 4+ GB.
- **Mac**: igual, desde Docker Desktop → Preferences → Resources.

---

## Variables de entorno configurables

Edita `.env` para cambiar cualquiera de estos valores:

| Variable | Defecto | Descripción |
|---|---|---|
| `MINIO_ACCESS_KEY` | `minioadmin` | Usuario MinIO |
| `MINIO_SECRET_KEY` | `minioadmin` | Contraseña MinIO |
| `POSTGRES_USER` | `postgres` | Usuario PostgreSQL |
| `POSTGRES_PASSWORD` | `postgres` | Contraseña PostgreSQL |
| `POSTGRES_DB` | `reviewsdb` | Nombre de la base de datos |
| `AIRFLOW_USER` | `airflow` | Usuario Airflow UI |
| `AIRFLOW_PASS` | `airflow` | Contraseña Airflow UI |

Los valores del producer (velocidad) y consumer (tamaño de lote) se configuran
directamente en el bloque `environment` de `docker-compose.yml`.

---

## Arquitectura de contenedores y dependencias

```
postgres ──────────────────────────────────┐
    │ healthy                               │ healthy
    ▼                                       ▼
airflow-init ──► airflow-webserver     jupyter_spark
                      │ healthy
                      ▼
              airflow-scheduler

rabbitmq ──────────────┐
    │ healthy           │ healthy
    ▼                   ▼
producer           minio ──► minio-init (exited 0)
                                  │ completed
                                  │
                     airflow-webserver (healthy)
                                  │
                                  ▼
                              consumer
                                  │
                              dashboard (postgres healthy)
```

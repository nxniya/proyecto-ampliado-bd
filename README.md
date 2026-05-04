# SentimentFlow — Análisis de Reseñas en Tiempo Real

Pipeline de Big Data **100 % local** que combina **RabbitMQ** (mensajería en tiempo real)
con una pila open-source desplegable en Docker para analizar el sentimiento de reseñas
de productos en tiempo casi-real.

---

## Arquitectura

```
[Python Producer] ──AMQP──► [RabbitMQ] ──consume──► [Python Consumer]
                                                            │
                                                    MinIO S3 (raw-reviews/*.jsonl)
                                                            │
                                               Airflow REST API (trigger DAG)
                                                            │
                                               ┌────────────────────────┐
                                               │  Apache Airflow         │
                                               │  DAG: reviews_etl       │
                                               │  ① sentiment_analysis   │
                                               │  ② aggregations         │
                                               └────────────────────────┘
                                                            │
                                               PySpark local (VADER)
                                               → processed-reviews/*.parquet
                                                            │
                                                   PostgreSQL Database
                                                            │
                                               [Streamlit Dashboard :8501]
```

### Equivalencias con la arquitectura Azure original

| Servicio Azure          | Alternativa local        |
|-------------------------|--------------------------|
| Azure Blob Storage      | **MinIO** (S3-compatible)|
| Azure Data Factory      | **Apache Airflow**       |
| Azure Databricks        | **PySpark local + Jupyter** |
| Azure SQL Database      | **PostgreSQL**           |

---

## Puesta en marcha

### 1. Requisitos previos
- Docker Desktop (o Docker Engine + Docker Compose v2)
- ~4 GB de RAM disponibles para los contenedores

### 2. Variables de entorno
```bash
cp .env.example .env
# Los valores por defecto ya funcionan para desarrollo local
```

### 3. Levantar toda la infraestructura
```bash
docker compose up -d
```

El primer arranque tarda varios minutos porque construye las imágenes (Airflow con Java+PySpark y Jupyter con PySpark). Los siguientes arranques son inmediatos.

### 4. Verificar el estado de los servicios
```bash
docker compose ps
```

Todos los servicios deben aparecer en estado `healthy` o `running`.

### 5. Servicios disponibles

| Servicio              | URL                            | Credenciales          |
|-----------------------|--------------------------------|-----------------------|
| RabbitMQ Management   | http://localhost:15672         | admin / admin123      |
| MinIO Console         | http://localhost:9001          | minioadmin / minioadmin |
| Airflow UI            | http://localhost:8080          | airflow / airflow     |
| JupyterLab (Spark)    | http://localhost:8888          | sin contraseña        |
| Dashboard Streamlit   | http://localhost:8501          | —                     |
| PostgreSQL            | localhost:5432                 | postgres / postgres   |

---

## Flujo de datos paso a paso

1. **Producer** genera reseñas sintéticas a 3 msg/s y las publica en `reviews_queue` de RabbitMQ.
2. **Consumer** acumula 100 mensajes (o 30 s) y sube un `.jsonl` al bucket `raw-reviews` de MinIO.
   Tras confirmar la subida, llama a la REST API de Airflow para activar el DAG `reviews_etl`.
3. **Airflow** ejecuta dos tareas secuenciales:
   - `sentiment_analysis`: descarga el `.jsonl` de MinIO, lo procesa con PySpark y VADER,
     escribe en PostgreSQL `reviews_sentiment` y sube el Parquet a `processed-reviews`.
   - `aggregations`: lee `reviews_sentiment` de PostgreSQL, calcula métricas con PySpark
     y actualiza las tablas `agg_by_product`, `agg_timeseries` y `agg_by_country`.
4. **Dashboard Streamlit** consulta PostgreSQL cada 30 s y muestra KPIs, gráficas y mapa.

---

## Estructura del proyecto

```
proyecto-ampliado/
├── docker-compose.yml          # Orquestación local (10 servicios)
├── .env.example                # Plantilla de variables de entorno
├── producer/                   # Generador de reseñas → RabbitMQ
├── consumer/                   # Consumidor RabbitMQ → MinIO + trigger Airflow
├── airflow/
│   ├── Dockerfile              # Imagen Airflow + Java + PySpark
│   └── dags/
│       └── dag_reviews_etl.py  # DAG que reemplaza el pipeline de ADF
├── spark/
│   ├── sentiment_analysis.py   # Job PySpark: VADER + PostgreSQL + MinIO
│   └── aggregations.py         # Job PySpark: métricas para el dashboard
├── notebooks/
│   ├── Dockerfile              # Imagen Jupyter + PySpark
│   ├── 01_sentiment_analysis.ipynb   # Versión interactiva del job 01
│   └── 02_aggregations_dashboard.ipynb # Versión interactiva del job 02
├── sql/
│   ├── 00_airflow_db.sql       # Crea la BD de metadatos de Airflow
│   └── schema.sql              # DDL del proyecto (PostgreSQL)
└── dashboard/                  # Streamlit dashboard
```

---

## Uso del entorno interactivo Jupyter

Accede a http://localhost:8888 y abre los notebooks de la carpeta `work/`:

- `01_sentiment_analysis.ipynb`: procesa manualmente un fichero `.jsonl` de MinIO.
  Cambia el valor de `INPUT_FILE` por el nombre de un fichero real del bucket.
- `02_aggregations_dashboard.ipynb`: recalcula los agregados del dashboard.

Los notebooks usan PySpark en modo local y se conectan a MinIO y PostgreSQL
usando las mismas variables de entorno que el pipeline automático.

---

## Activar el DAG de Airflow manualmente

Si quieres procesar un fichero que ya está en MinIO sin esperar al consumer:

```bash
# Via CLI de Airflow
docker exec airflow_scheduler airflow dags trigger reviews_etl \
  --conf '{"input_file": "batch_20260501T120000_0100.jsonl"}'

# Via REST API (curl)
curl -X POST http://localhost:8080/api/v1/dags/reviews_etl/dagRuns \
  -H "Content-Type: application/json" \
  -u airflow:airflow \
  -d '{"conf": {"input_file": "batch_20260501T120000_0100.jsonl"}}'
```

---

## Tecnologías utilizadas

| Capa              | Tecnología                   | Rol                                         |
|-------------------|------------------------------|---------------------------------------------|
| Ingesta streaming | **RabbitMQ 3.12**            | Cola de mensajes AMQP durable               |
| Productores       | Python + Faker               | Genera eventos de reseñas sintéticas        |
| Bridge            | Python + pika + boto3        | Lee de RabbitMQ, sube lotes a MinIO         |
| Data Lake (raw)   | **MinIO**                    | Almacenamiento S3-compatible local          |
| Orquestación      | **Apache Airflow**           | Pipeline ETL activado por evento del consumer |
| Procesamiento     | **PySpark local** + VADER    | Limpieza + análisis de sentimiento          |
| Data Lake (proc.) | **MinIO**                    | Zona de datos procesados (Parquet/Snappy)   |
| Data Warehouse    | **PostgreSQL**               | Resultados y agregaciones                   |
| Exploración       | **JupyterLab + PySpark**     | Notebooks interactivos                      |
| Visualización     | Streamlit + Plotly           | Dashboard interactivo                       |

---

## Reparto de trabajo

| Persona   | Responsabilidades                                                                   |
|-----------|-------------------------------------------------------------------------------------|
| Persona 1 | Producer (Python/RabbitMQ), Consumer (Python/MinIO/Airflow), Docker Compose, README |
| Persona 2 | DAG Airflow, Scripts PySpark (spark/), Notebooks Jupyter, Schema PostgreSQL, Dashboard |

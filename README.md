# SentimentFlow — Análisis de Reseñas en Tiempo Real

Pipeline de Big Data que combina **RabbitMQ** (mensajería en tiempo real) con el ecosistema
de datos de **Microsoft Azure** (Blob Storage, Data Factory, Databricks, SQL Database) para
analizar el sentimiento de reseñas de productos en tiempo casi-real.

---

## Arquitectura

```
[Python Producer] ──AMQP──► [RabbitMQ] ──consume──► [Python Consumer]
                                                            │
                                                    Azure Blob Storage
                                                     (raw-reviews/*.jsonl)
                                                            │
                                               Storage Event Trigger (ADF)
                                                            │
                                               ┌────────────────────────┐
                                               │  Azure Data Factory     │
                                               │  pipeline_reviews_etl   │
                                               │  ① DatabricksNotebook   │
                                               │  ② Copy → Azure SQL     │
                                               └────────────────────────┘
                                                            │
                                               Azure Databricks (VADER)
                                               → processed-reviews/*.parquet
                                                            │
                                                   Azure SQL Database
                                                            │
                                               [Streamlit Dashboard :8501]
```

## Puesta en marcha rápida

### 1. Requisitos previos
- Docker Desktop
- Cuenta de Azure con los servicios: Blob Storage, Data Factory, Databricks (Community), SQL Database
- Python 3.11+ (solo para pruebas locales sin Docker)

### 2. Variables de entorno
```bash
cp .env.example .env
# Editar .env con las credenciales reales de Azure
```

### 3. Levantar la infraestructura local
```bash
docker compose up -d
```

Servicios disponibles:
| Servicio              | URL                          |
|-----------------------|------------------------------|
| RabbitMQ Management   | http://localhost:15672        |
| Dashboard Streamlit   | http://localhost:8501         |

### 4. Esquema de base de datos
Ejecutar `sql/schema.sql` en Azure SQL Database (Azure Portal → Query Editor).

### 5. Importar configuración ADF
En Azure Data Factory Studio → **Manage → Git configuration** o importar los JSON de `adf/`
usando `az datafactory` CLI:
```bash
az datafactory linked-service create --factory-name <ADF> -g <RG> \
  --name ls_AzureBlobStorage --properties @adf/linkedService/ls_AzureBlobStorage.json
# Repetir para cada linked service, dataset, pipeline y trigger
```

### 6. Cargar notebooks en Databricks
1. Databricks UI → Workspace → Import → subir `databricks/01_sentiment_analysis.ipynb`
2. Subir `databricks/02_aggregations_dashboard.ipynb`
3. Configurar Databricks Secrets con las claves de Azure Storage y SQL.

---

## Estructura del proyecto

```
proyecto-ampliado/
├── docker-compose.yml          # Orquestación local
├── .env.example                # Plantilla de variables de entorno
├── producer/                   # Generador de reseñas → RabbitMQ
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/                   # Consumidor RabbitMQ → Azure Blob
│   ├── consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── adf/                        # Azure Data Factory (JSON exportados)
│   ├── linkedService/
│   ├── dataset/
│   ├── pipeline/
│   └── trigger/
├── databricks/                 # Notebooks PySpark + VADER
│   ├── 01_sentiment_analysis.ipynb
│   └── 02_aggregations_dashboard.ipynb
├── sql/
│   └── schema.sql              # DDL de Azure SQL Database
└── dashboard/                  # Streamlit dashboard
    ├── app.py
    ├── Dockerfile
    └── requirements.txt
```

---

## Tecnologías utilizadas

| Capa              | Tecnología                        | Rol                                       |
|-------------------|-----------------------------------|-------------------------------------------|
| Ingesta streaming | **RabbitMQ 3.12**                 | Cola de mensajes AMQP durable             |
| Productores       | Python + Faker                    | Genera eventos de reseñas                 |
| Bridge            | Python + pika + azure-storage-blob| Lee de RabbitMQ, sube lotes a Blob        |
| Data Lake (raw)   | Azure Blob Storage                | Zona de aterrizaje de ficheros .jsonl     |
| Orquestación      | **Azure Data Factory**            | Pipeline ETL activado por evento de Blob  |
| Procesamiento     | **Azure Databricks** + PySpark    | Limpieza + análisis de sentimiento (VADER)|
| Data Lake (proc.) | Azure Blob Storage                | Zona de datos procesados (Parquet/Snappy) |
| Data Warehouse    | Azure SQL Database                | Resultados y agregaciones                 |
| Visualización     | Streamlit + Plotly                | Dashboard interactivo                     |

---

## Reparto de trabajo

| Persona   | Responsabilidades                                                                   |
|-----------|-------------------------------------------------------------------------------------|
| Persona 1 | Producer (Python/RabbitMQ), Consumer (Python/Azure Blob), Docker Compose, README    |
| Persona 2 | ADF pipeline + trigger, Databricks notebooks, SQL schema, Dashboard Streamlit       |

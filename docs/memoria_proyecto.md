# Memoria del Proyecto — SentimentFlow

**Asignatura:** Big Data  
**Grupo:** [Nombres y apellidos]  
**Fecha de entrega:** 6 de mayo de 2026

---

## 1. Objetivo del trabajo

El objetivo de SentimentFlow es construir un **pipeline de procesamiento de datos en tiempo casi-real** que capture reseñas de productos de una plataforma de e-commerce, analice automáticamente su sentimiento (positivo / neutro / negativo) y presente los resultados en un dashboard interactivo actualizado cada 30 segundos.

El proyecto demuestra la integración de herramientas de mensajería orientada a eventos (**RabbitMQ**) con el ecosistema de datos de **Microsoft Azure** (Data Factory, Databricks, Blob Storage, SQL Database), formando una arquitectura Lambda simplificada.

---

## 2. Tecnología elegida y justificación

### RabbitMQ

RabbitMQ es un broker de mensajería AMQP de código abierto, ampliamente utilizado en arquitecturas de Big Data para desacoplar productores de consumidores y garantizar la entrega de mensajes incluso ante caídas temporales de algún componente.

**Justificación de elección:**
- Soporte nativo de colas durables y confirmaciones de entrega (ACK), lo que garantiza que ninguna reseña se pierda si el consumer falla.
- Protocolo AMQP estándar con clientes Python maduros (`pika`).
- Interfaz de administración web que permite monitorizar la profundidad de las colas en tiempo real.
- Relevante en el contexto Big Data como capa de ingesta ante picos de carga (buffer ante ráfagas de eventos).

### Azure Data Factory (ADF)

ADF es el servicio de orquestación ETL/ELT de Azure. Permite definir **pipelines** visuales que combinan actividades (notebooks, copias de datos, procedimientos almacenados) y dispararlos con **triggers** basados en eventos de almacenamiento.

**Justificación de elección:**
- Orquestación serverless: no requiere gestionar infraestructura de scheduler.
- Storage Event Trigger: reacciona de forma nativa cuando el consumer deposita un nuevo fichero en Blob Storage.
- Integración directa con Azure Databricks como actividad del pipeline.
- Reintento automático de actividades fallidas y registro de errores.

### Azure Databricks

Databricks es una plataforma de análisis de datos basada en Apache Spark. Se usa aquí para el procesamiento distribuido de los lotes de reseñas y la ejecución del modelo de análisis de sentimiento.

**Justificación de elección:**
- PySpark permite escalar el procesamiento a millones de reseñas si fuera necesario.
- VADER (Valence Aware Dictionary and Sentiment Reasoner) es un modelo de análisis de sentimiento pre-entrenado, ligero y efectivo para textos cortos en idiomas con recursos léxicos limitados.
- Los notebooks son reproducibles y auditables.

---

## 3. Arquitectura y flujo de datos

```
[Python Producer]  →  RabbitMQ  →  [Python Consumer]  →  Azure Blob Storage (raw)
                                                                  ↓
                                                   ADF Storage Event Trigger
                                                                  ↓
                                           Azure Data Factory Pipeline
                                           ① DatabricksNotebook (VADER)
                                           ② Copy Activity → Azure SQL
                                                                  ↓
                                                   Azure SQL Database
                                                                  ↓
                                                  Streamlit Dashboard
```

**Descripción paso a paso:**

1. El **Producer** genera reseñas sintéticas con datos aleatorios (producto, valoración, texto, país, timestamp) y las publica en la cola `reviews_queue` de RabbitMQ a razón de 3 msg/s.

2. **RabbitMQ** almacena los mensajes de forma durable hasta que el consumer los consume y confirma (ACK). La cola actúa como buffer ante posibles latencias en la subida a Azure.

3. El **Consumer** lee mensajes de la cola con `prefetch=1` (fair dispatch), los acumula en un buffer thread-safe y cuando se alcanzan 100 mensajes (o 30 segundos de espera) sube un fichero `.jsonl` al contenedor `raw-reviews` de Azure Blob Storage. Solo hace ACK a RabbitMQ tras confirmar que la subida ha sido exitosa.

4. El **Storage Event Trigger** de Azure Data Factory detecta la creación del nuevo fichero y lanza el pipeline `pipeline_reviews_etl`, pasando el nombre del fichero como parámetro.

5. **Azure Data Factory** ejecuta secuencialmente:
   - **Actividad 1** (`DatabricksNotebook`): invoca el notebook `01_sentiment_analysis` en el clúster de Databricks, que lee el `.jsonl`, limpia los textos, aplica VADER para calcular el score de sentimiento de cada reseña y escribe el resultado en Parquet con compresión Snappy en el contenedor `processed-reviews`.
   - **Actividad 2** (`Copy`): copia el Parquet generado a la tabla `dbo.reviews_sentiment` de Azure SQL Database.
   - En caso de fallo de la actividad 1, una actividad de error llama al procedimiento almacenado `sp_log_pipeline_error` para registrar el fallo.

6. El **notebook 02** (ejecutado a demanda o en cron) calcula agregaciones por producto, por hora y por país, escribiéndolas en tablas auxiliares de Azure SQL que el dashboard consulta.

7. El **dashboard Streamlit** consulta Azure SQL cada 30 segundos y presenta KPIs, gráfico de barras por producto, serie temporal de sentimiento, mapa de calor por países y una tabla de las últimas 200 reseñas procesadas.

---

## 4. Explicación del desarrollo realizado

### Persona 1 — Capa de ingesta (RabbitMQ)

- Implementó el **Producer** (`producer/producer.py`) usando la biblioteca `pika` y `Faker` para generar reseñas sintéticas con distribución de ratings realista (sesgo hacia valoraciones altas).
- Configuró las colas como **durables** y los mensajes como **persistentes** para garantizar que no se pierdan ante un reinicio del broker.
- Implementó el **Consumer** (`consumer/consumer.py`) con un buffer thread-safe que acumula mensajes y hace flush por tamaño de lote o por timeout, subiendo ficheros `.jsonl` a Azure Blob Storage mediante la biblioteca `azure-storage-blob`.
- El consumer sólo hace ACK a RabbitMQ **después** de confirmar la subida al blob, garantizando la consistencia "at-least-once" del pipeline.
- Configuró el entorno Docker Compose con health checks para que los contenedores del producer y consumer esperen a que RabbitMQ esté listo antes de arrancar.

### Persona 2 — Capa de procesamiento y visualización (Azure)

- Diseñó el **esquema de base de datos** en Azure SQL con tablas de detalle, agregados y auditoría de errores (`sql/schema.sql`).
- Configuró los **Linked Services, Datasets, Pipeline y Trigger** de Azure Data Factory (JSON en `adf/`) que conectan Blob Storage, Databricks y Azure SQL.
- Desarrolló el **notebook 01** (`databricks/01_sentiment_analysis.ipynb`) en PySpark que lee el `.jsonl`, limpia los textos con una UDF, aplica VADER con otra UDF y escribe Parquet.
- Desarrolló el **notebook 02** (`databricks/02_aggregations_dashboard.ipynb`) que agrega los datos históricos en tres dimensiones (producto, hora, país) y los escribe en Azure SQL vía JDBC.
- Implementó el **dashboard Streamlit** (`dashboard/app.py`) con 4 KPIs, gráfico de barras interactivo (Plotly), gráfico de tarta de distribución de sentimiento, serie temporal y mapa de países.

---

## 5. Evidencias de funcionamiento

_(Sustituir por capturas de pantalla reales)_

- **Captura 1:** RabbitMQ Management UI mostrando la cola `reviews_queue` con mensajes en tránsito.
- **Captura 2:** Azure Blob Storage con varios ficheros `.jsonl` en el contenedor `raw-reviews`.
- **Captura 3:** Azure Data Factory — pipeline `pipeline_reviews_etl` con una ejecución exitosa (color verde).
- **Captura 4:** Azure Databricks — output del notebook 01 con la distribución de sentimiento del lote.
- **Captura 5:** Azure SQL Database — tabla `reviews_sentiment` con filas procesadas.
- **Captura 6:** Dashboard Streamlit con KPIs, gráficas y mapa de países.

---

## 6. Dificultades encontradas y soluciones aplicadas

| Dificultad | Solución |
|---|---|
| El consumer hacía ACK antes de confirmar la subida al Blob, perdiendo mensajes si Azure fallaba | Se movió el ACK a RabbitMQ **después** de que `upload_blob()` completara sin excepción |
| El Storage Event Trigger de ADF requiere una suscripción de eventos de Azure registrada en el Resource Provider | Registrar `Microsoft.EventGrid` en la suscripción de Azure desde el Portal |
| VADER no analiza bien el español (fue entrenado en inglés) | Se añadió una nota en el notebook; para producción real se podría usar `pysentimiento` o traducir con Azure Cognitive Services |
| Docker Compose arrancaba el producer antes de que RabbitMQ estuviera listo | Se añadió `healthcheck` al servicio RabbitMQ y `condition: service_healthy` en las dependencias |
| El clúster de Databricks tardaba en arrancar (cold start ~5 min) | Se activó el modo "auto-terminate after 30 min" y se usó un clúster pre-arrancado para demos |

---

## 7. Reparto aproximado del trabajo

| Tarea | Persona 1 | Persona 2 |
|---|---|---|
| Producer (RabbitMQ) | ✓ | |
| Consumer + bridge Azure | ✓ | |
| Docker Compose | ✓ | |
| Esquema Azure SQL | | ✓ |
| Linked Services / Datasets ADF | | ✓ |
| Pipeline y Trigger ADF | | ✓ |
| Notebook 01 (sentimiento) | | ✓ |
| Notebook 02 (agregaciones) | | ✓ |
| Dashboard Streamlit | | ✓ |
| README y documentación | ✓ | ✓ |

---

## 8. Conclusiones y posibles mejoras

**Conclusiones:**  
SentimentFlow demuestra cómo integrar un sistema de mensajería clásico como RabbitMQ con los servicios gestionados de Azure para construir un pipeline de datos robusto, escalable y con tolerancia a fallos. La combinación de ADF (orquestación) y Databricks (procesamiento) es habitual en arquitecturas Big Data empresariales.

**Posibles mejoras:**
- Sustituir el batch de 100 mensajes por **Azure Event Hubs** con procesamiento de Spark Structured Streaming para reducir la latencia a segundos.
- Usar **Azure Cognitive Services (Text Analytics)** para un análisis de sentimiento multilingüe de mayor precisión.
- Añadir una capa de **Data Quality** con Great Expectations antes de la escritura en SQL.
- Desplegar la infraestructura con **Terraform** o **Bicep** para reproducibilidad.
- Implementar alertas (Azure Monitor) cuando el sentimiento medio de un producto cae por debajo de un umbral.

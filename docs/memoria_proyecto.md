# Memoria del Proyecto — SentimentFlow

**Asignatura:** Big Data  
**Grupo:** [Nombres y apellidos]  
**Fecha de entrega:** 6 de mayo de 2026

---

## 1. Objetivo del trabajo

El objetivo de SentimentFlow es construir un **pipeline de procesamiento de datos en tiempo casi-real** que capture reseñas de productos de una plataforma de e-commerce, analice automáticamente su sentimiento (positivo / neutro / negativo) y presente los resultados en un dashboard interactivo actualizado cada 30 segundos.

El proyecto demuestra la integración de herramientas de mensajería orientada a eventos (**RabbitMQ**) con una pila de Big Data open-source completamente desplegable en local mediante Docker, formando una arquitectura Lambda simplificada.

---

## 2. Tecnología elegida y justificación

### RabbitMQ

RabbitMQ es un broker de mensajería AMQP de código abierto, ampliamente utilizado en arquitecturas de Big Data para desacoplar productores de consumidores y garantizar la entrega de mensajes incluso ante caídas temporales.

**Justificación:**
- Colas **durables** y mensajes **persistentes**: no se pierde ninguna reseña si el broker se reinicia.
- ACK manual: el consumer solo confirma el mensaje cuando la subida a MinIO ha sido exitosa, garantizando semántica *at-least-once*.
- Interfaz de administración web para monitorizar la profundidad de las colas en tiempo real.

### MinIO (reemplaza Azure Blob Storage)

MinIO es un servidor de almacenamiento de objetos compatible con la API de Amazon S3. Se despliega en un contenedor Docker y actúa como Data Lake local.

**Justificación:**
- API S3-compatible: el código Python usa `boto3` exactamente igual que usaría el SDK de AWS o Azure Blob Storage.
- Interfaz web para explorar los buckets y ficheros subidos.
- Sin coste, sin dependencias de red externa, reproducible en cualquier máquina.
- Escalable: en producción se podría sustituir por AWS S3 o Azure Blob Storage cambiando solo el endpoint.

### Apache Airflow (reemplaza Azure Data Factory)

Airflow es el estándar de facto para la orquestación de pipelines de datos. Define los flujos como DAGs (Directed Acyclic Graphs) en Python.

**Justificación:**
- DAGs como código: reproducibles, versionables con git, testables.
- El consumer activa el DAG mediante la **REST API de Airflow** justo al subir cada fichero, replicando el comportamiento event-driven del Storage Event Trigger de ADF.
- Interfaz web con historial de ejecuciones, logs por tarea y reintentos configurables.
- Sin coste, sin dependencias de red externa.

### Apache Spark / JupyterLab (reemplaza Azure Databricks)

PySpark se ejecuta en modo local dentro del contenedor de Airflow. JupyterLab proporciona un entorno de notebooks interactivo equivalente al de Databricks.

**Justificación:**
- PySpark local permite procesar los lotes de reseñas con el mismo API que usaría un clúster Databricks a escala.
- VADER (Valence Aware Dictionary and Sentiment Reasoner): modelo NLP preentrenado, ligero y sin coste de API, adecuado para textos cortos.
- Los notebooks son reproducibles y permiten exploración interactiva de los datos.

### PostgreSQL (reemplaza Azure SQL Database)

PostgreSQL es el sistema de gestión de bases de datos relacionales open-source más completo.

**Justificación:**
- DDL estándar SQL compatible con la mayoría de herramientas de visualización.
- `psycopg2` es el driver Python de referencia, ligero y sin dependencias externas.
- Sin coste, sin límites de consulta, sin necesidad de driver ODBC propietario.
- El esquema de tablas (`reviews_sentiment`, `agg_*`) es idéntico al diseñado para Azure SQL, solo cambia la sintaxis T-SQL → PostgreSQL.

---

## 3. Arquitectura y flujo de datos

```
[Python Producer]  →  RabbitMQ  →  [Python Consumer]  →  MinIO (raw-reviews/*.jsonl)
                                                                  ↓
                                                   Consumer llama REST API Airflow
                                                                  ↓
                                           Apache Airflow DAG: reviews_etl
                                           ① Task: sentiment_analysis (PySpark)
                                           ② Task: aggregations (PySpark)
                                                                  ↓
                                                   PostgreSQL Database
                                                                  ↓
                                                  Streamlit Dashboard
```

**Descripción paso a paso:**

1. El **Producer** genera reseñas sintéticas con datos aleatorios (producto, valoración, texto, país, timestamp) y las publica en la cola `reviews_queue` de RabbitMQ a razón de 3 msg/s.

2. **RabbitMQ** almacena los mensajes de forma durable hasta que el consumer los consume y confirma (ACK). La cola actúa como buffer ante posibles latencias en la subida a MinIO.

3. El **Consumer** lee mensajes de la cola con `prefetch=1` (fair dispatch), los acumula en un buffer thread-safe y cuando se alcanzan 100 mensajes (o 30 segundos de espera) sube un fichero `.jsonl` al bucket `raw-reviews` de MinIO usando `boto3`. Solo hace ACK a RabbitMQ tras confirmar que la subida ha sido exitosa. Acto seguido, llama a la REST API de Airflow para activar el DAG `reviews_etl` pasando el nombre del fichero como parámetro.

4. **Apache Airflow** recibe la llamada y crea una nueva ejecución del DAG, ejecutando secuencialmente:
   - **Task 1** (`sentiment_analysis`): descarga el `.jsonl` de MinIO a un directorio temporal, lo lee con PySpark, limpia los textos mediante UDFs, aplica VADER para calcular el score de sentimiento, escribe los resultados en PostgreSQL (`reviews_sentiment`) y sube el Parquet resultante al bucket `processed-reviews` de MinIO.
   - **Task 2** (`aggregations`): lee todos los registros de `reviews_sentiment` desde PostgreSQL, calcula agregados por producto, por hora y por país usando PySpark, y actualiza las tablas `agg_by_product`, `agg_timeseries` y `agg_by_country`.

5. El **dashboard Streamlit** consulta PostgreSQL cada 30 segundos y presenta KPIs, gráfico de barras por producto, distribución de sentimiento, serie temporal y mapa de países.

6. **JupyterLab** está disponible en el puerto 8888 para ejecutar los notebooks interactivos de procesamiento y exploración, que implementan la misma lógica que los scripts de Airflow pero con visualización de resultados intermedios.

---

## 4. Explicación del desarrollo realizado

### Persona 1 — Capa de ingesta (RabbitMQ + Consumer)

- Implementó el **Producer** (`producer/producer.py`) usando `pika` y `Faker` para generar reseñas sintéticas con distribución de ratings realista (sesgo hacia valoraciones altas).
- Configuró las colas como **durables** y los mensajes como **persistentes** para garantizar que no se pierdan ante un reinicio del broker.
- Implementó el **Consumer** (`consumer/consumer.py`) con un buffer thread-safe que acumula mensajes y hace flush por tamaño de lote o por timeout, subiendo ficheros `.jsonl` a MinIO mediante `boto3`.
- Integró la llamada a la **REST API de Airflow** desde el consumer, replicando el comportamiento event-driven del Storage Event Trigger de ADF.
- Configuró el entorno Docker Compose con health checks para que los servicios arranquen en el orden correcto.

### Persona 2 — Capa de procesamiento y visualización

- Diseñó el **esquema de base de datos** en PostgreSQL con tablas de detalle, agregados y auditoría de errores (`sql/schema.sql`).
- Configuró el **DAG de Airflow** (`airflow/dags/dag_reviews_etl.py`) con las dos tareas del pipeline y la gestión de errores mediante reintentos automáticos.
- Desarrolló el **script de análisis de sentimiento** (`spark/sentiment_analysis.py`) en PySpark que lee el `.jsonl`, limpia los textos con UDFs, aplica VADER y escribe en PostgreSQL y MinIO.
- Desarrolló el **script de agregaciones** (`spark/aggregations.py`) que calcula métricas en tres dimensiones (producto, hora, país) y las escribe en PostgreSQL.
- Adaptó los **notebooks Jupyter** (`notebooks/`) para exploración interactiva equivalente a los notebooks originales de Databricks.
- Implementó el **dashboard Streamlit** (`dashboard/app.py`) conectado a PostgreSQL mediante `psycopg2`.

---

## 5. Evidencias de funcionamiento

_(Sustituir por capturas de pantalla reales)_

- **Captura 1:** RabbitMQ Management UI (`http://localhost:15672`) mostrando la cola `reviews_queue` con mensajes en tránsito.
- **Captura 2:** MinIO Console (`http://localhost:9001`) con varios ficheros `.jsonl` en el bucket `raw-reviews`.
- **Captura 3:** Airflow UI (`http://localhost:8080`) — DAG `reviews_etl` con varias ejecuciones exitosas (estado verde).
- **Captura 4:** Detalle de una ejecución del DAG mostrando las dos tareas (`sentiment_analysis` y `aggregations`) en verde.
- **Captura 5:** JupyterLab con el notebook 01 mostrando la distribución de sentimiento del lote.
- **Captura 6:** PostgreSQL — resultado de `SELECT COUNT(*), sentiment_label FROM reviews_sentiment GROUP BY sentiment_label`.
- **Captura 7:** Dashboard Streamlit con KPIs, gráficas y mapa de países.

---

## 6. Dificultades encontradas y soluciones aplicadas

| Dificultad | Solución |
|---|---|
| El consumer hacía ACK antes de confirmar la subida, perdiendo mensajes si MinIO fallaba | Se movió el ACK a RabbitMQ **después** de que `s3.put_object()` completara sin excepción |
| El trigger event-driven de ADF (Storage Event Trigger) no tiene equivalente nativo en MinIO | Se implementó la llamada a la **REST API de Airflow** desde el consumer, manteniendo la arquitectura event-driven |
| El Airflow webserver tarda en arrancar; el consumer intentaba activar el DAG antes de que estuviera listo | Se añadió `condition: service_healthy` en el `depends_on` del consumer hacia el webserver de Airflow |
| VADER no analiza bien el español (fue entrenado en inglés) | Documentado como limitación; para producción real se recomendaría `pysentimiento` |
| Docker Compose arrancaba servicios antes de que sus dependencias estuvieran listas | Se añadieron `healthcheck` a todos los servicios críticos y se usó `condition: service_healthy` / `service_completed_successfully` en los `depends_on` |
| El schema.sql de T-SQL (Azure SQL) es incompatible con PostgreSQL | Se reescribió el DDL completo en sintaxis PostgreSQL (`VARCHAR`, `TIMESTAMP`, `SERIAL`, `NOW()`, sin `GO`) |

---

## 7. Reparto aproximado del trabajo

| Tarea | Persona 1 | Persona 2 |
|---|---|---|
| Producer (RabbitMQ) | ✓ | |
| Consumer + bridge MinIO + trigger Airflow | ✓ | |
| Docker Compose | ✓ | |
| Esquema PostgreSQL | | ✓ |
| DAG de Airflow | | ✓ |
| Script PySpark: sentiment_analysis | | ✓ |
| Script PySpark: aggregations | | ✓ |
| Notebooks Jupyter | | ✓ |
| Dashboard Streamlit | | ✓ |
| README y documentación | ✓ | ✓ |

---

## 8. Conclusiones y posibles mejoras

**Conclusiones:**  
SentimentFlow demuestra cómo construir un pipeline de datos en tiempo casi-real con herramientas open-source sin depender de servicios cloud de pago. La arquitectura local es funcionalmente equivalente a la basada en Azure y facilita el desarrollo, las pruebas y la presentación del proyecto sin restricciones de créditos cloud.

**Posibles mejoras:**
- Sustituir el batch de 100 mensajes por **Apache Kafka** con Spark Structured Streaming para latencia de segundos.
- Usar **pysentimiento** (modelo entrenado en español/multilingüe) para mayor precisión en el análisis de sentimiento.
- Añadir una capa de **Data Quality** con Great Expectations antes de la escritura en PostgreSQL.
- Desplegar la infraestructura con **Docker Swarm** o **Kubernetes** para escalar horizontalmente.
- Implementar alertas (Airflow callbacks + Slack) cuando el sentimiento medio de un producto cae por debajo de un umbral.
- En entornos cloud reales, sustituir MinIO → S3/Azure Blob, PostgreSQL → RDS/Azure SQL, Airflow → MWAA/Cloud Composer, cambiando solo las variables de entorno.

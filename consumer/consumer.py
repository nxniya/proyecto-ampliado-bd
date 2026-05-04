"""
consumer.py — Consumidor RabbitMQ → MinIO (S3-compatible).

Lee mensajes de la cola 'reviews_queue', los acumula en un buffer
y cuando se alcanza BATCH_SIZE mensajes (o BATCH_TIMEOUT_SECONDS segundos)
sube un fichero JSON Lines a MinIO en el bucket 'raw-reviews'.

Tras cada subida exitosa activa el DAG de Airflow mediante su REST API,
replicando el comportamiento del Storage Event Trigger de Azure Data Factory.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from io import BytesIO

import pika
import boto3
import requests
from botocore.client import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER  = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS  = os.getenv("RABBITMQ_PASS", "admin123")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "reviews_queue")
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT  = int(os.getenv("BATCH_TIMEOUT_SECONDS", "30"))

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "raw-reviews")

AIRFLOW_URL  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "airflow")
AIRFLOW_DAG  = "reviews_etl"


# ─── Gestor de lotes (thread-safe) ────────────────────────────────────────────
class BatchUploader:
    """Acumula mensajes y los sube a MinIO en lotes."""

    def __init__(self):
        self._buffer: list = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

        self._s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self._s3.create_bucket(Bucket=MINIO_BUCKET_RAW)
            log.info("Bucket '%s' creado.", MINIO_BUCKET_RAW)
        except self._s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        except Exception:
            pass  # El bucket puede existir ya (de minio-init)

    def add(self, message: dict, ack_fn):
        with self._lock:
            self._buffer.append((message, ack_fn))
            should_flush = (
                len(self._buffer) >= BATCH_SIZE
                or (time.monotonic() - self._last_flush) >= BATCH_TIMEOUT
            )
        if should_flush:
            self._flush()

    def _flush(self):
        with self._lock:
            if not self._buffer:
                return
            batch = self._buffer.copy()
            self._buffer.clear()
            self._last_flush = time.monotonic()

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        object_name = f"batch_{ts}_{len(batch):04d}.jsonl"

        jsonl_content = "\n".join(
            json.dumps(msg, ensure_ascii=False) for msg, _ in batch
        ).encode("utf-8")

        try:
            self._s3.put_object(
                Bucket=MINIO_BUCKET_RAW,
                Key=object_name,
                Body=BytesIO(jsonl_content),
                ContentType="application/x-ndjson",
            )
            log.info(
                "✓ Subido '%s' a MinIO (%d reseñas, %.1f KB)",
                object_name, len(batch), len(jsonl_content) / 1024,
            )
            # ACK a RabbitMQ solo tras confirmar la subida
            for _, ack_fn in batch:
                ack_fn()

            self._trigger_airflow(object_name)

        except Exception as exc:
            log.error("Error al subir '%s': %s. Mensajes NO confirmados.", object_name, exc)

    def _trigger_airflow(self, filename: str):
        """Activa el DAG de Airflow pasando el nombre del fichero como parámetro."""
        url = f"{AIRFLOW_URL}/api/v1/dags/{AIRFLOW_DAG}/dagRuns"
        try:
            resp = requests.post(
                url,
                json={"conf": {"input_file": filename}},
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                timeout=10,
            )
            resp.raise_for_status()
            log.info("DAG '%s' activado para: %s", AIRFLOW_DAG, filename)
        except Exception as exc:
            log.warning(
                "No se pudo activar el DAG de Airflow: %s. "
                "El fichero está en MinIO y puede procesarse manualmente.",
                exc,
            )


# ─── Conexión RabbitMQ ─────────────────────────────────────────────────────────
def connect_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    for attempt in range(1, 11):
        try:
            conn = pika.BlockingConnection(params)
            log.info("Conectado a RabbitMQ en %s", RABBITMQ_HOST)
            return conn
        except pika.exceptions.AMQPConnectionError as exc:
            log.warning("Intento %d/10 fallido: %s. Reintentando en 5 s…", attempt, exc)
            time.sleep(5)
    raise RuntimeError("No se pudo conectar a RabbitMQ tras 10 intentos.")


# ─── Timer periódico de flush ──────────────────────────────────────────────────
def start_periodic_flush(uploader: BatchUploader):
    """Hilo daemon que fuerza un flush cada BATCH_TIMEOUT segundos."""
    def _loop():
        while True:
            time.sleep(BATCH_TIMEOUT)
            uploader._flush()  # noqa: SLF001

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    uploader = BatchUploader()
    start_periodic_flush(uploader)

    connection = connect_rabbitmq()
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, _properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            log.error("Mensaje inválido ignorado: %s", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)

        uploader.add(message, ack)

    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message)
    log.info(
        "Esperando mensajes en '%s'. Lote: %d msgs / %d s…",
        RABBITMQ_QUEUE, BATCH_SIZE, BATCH_TIMEOUT,
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("Consumer detenido.")
        channel.stop_consuming()
    finally:
        uploader._flush()  # noqa: SLF001
        connection.close()


if __name__ == "__main__":
    main()

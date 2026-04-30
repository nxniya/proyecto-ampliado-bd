"""
consumer.py — Consumidor RabbitMQ → Azure Blob Storage.

Lee mensajes de la cola 'reviews_queue', los acumula en un buffer
y cuando se alcanza BATCH_SIZE mensajes (o BATCH_TIMEOUT_SECONDS segundos)
sube un fichero JSON Lines a Azure Blob Storage en el contenedor 'raw-reviews'.

Ese fichero activa automáticamente el pipeline de Azure Data Factory
mediante un Storage Event Trigger configurado en ADF.

Responsable: Persona 1
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from io import BytesIO

import pika
from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
RABBITMQ_HOST    = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER    = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS    = os.getenv("RABBITMQ_PASS", "admin123")
RABBITMQ_QUEUE   = os.getenv("RABBITMQ_QUEUE", "reviews_queue")
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT    = int(os.getenv("BATCH_TIMEOUT_SECONDS", "30"))

AZURE_CONN_STR   = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_CONTAINER  = os.getenv("AZURE_CONTAINER_RAW", "raw-reviews")


# ─── Gestor de lotes (thread-safe) ────────────────────────────────────────────
class BatchUploader:
    """Acumula mensajes y los sube a Blob Storage en lotes."""

    def __init__(self):
        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

        if not AZURE_CONN_STR:
            raise EnvironmentError(
                "AZURE_STORAGE_CONNECTION_STRING no está definida. "
                "Copia .env.example a .env y rellena los valores."
            )
        self._blob_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        # Crea el contenedor si no existe
        container = self._blob_client.get_container_client(AZURE_CONTAINER)
        if not container.exists():
            container.create_container()
            log.info("Contenedor '%s' creado.", AZURE_CONTAINER)

    def add(self, message: dict, ack_fn):
        """Añade un mensaje al buffer. Hace flush si se cumple algún criterio."""
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
        blob_name = f"batch_{ts}_{len(batch):04d}.jsonl"

        # JSON Lines: una reseña por línea
        jsonl_content = "\n".join(
            json.dumps(msg, ensure_ascii=False) for msg, _ in batch
        ).encode("utf-8")

        try:
            blob = self._blob_client.get_blob_client(
                container=AZURE_CONTAINER, blob=blob_name
            )
            blob.upload_blob(BytesIO(jsonl_content), overwrite=True)
            log.info("✓ Subido '%s' (%d reseñas, %.1f KB)",
                     blob_name, len(batch), len(jsonl_content) / 1024)
            # ACK a RabbitMQ solo tras confirmar la subida
            for _, ack_fn in batch:
                ack_fn()
        except Exception as exc:  # noqa: BLE001
            log.error("Error al subir '%s': %s. Mensajes NO confirmados.", blob_name, exc)


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

    # Procesar un mensaje a la vez (prefetch=1 → fair dispatch)
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
    log.info("Esperando mensajes en '%s'. Lote: %d msgs / %d s…",
             RABBITMQ_QUEUE, BATCH_SIZE, BATCH_TIMEOUT)
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

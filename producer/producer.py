"""
producer.py — Generador de reseñas de productos sintéticas.

Simula el flujo continuo de reseñas de usuarios en una plataforma de e-commerce
y las publica en RabbitMQ (exchange directo, cola 'reviews_queue').

Responsable: Persona 1
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pika
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "admin123")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "reviews_queue")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "3"))

# ─── Datos de productos de ejemplo ────────────────────────────────────────────
PRODUCTS = [
    {"id": "P001", "name": "Auriculares Bluetooth XZ-500", "category": "Electrónica"},
    {"id": "P002", "name": "Zapatillas Running ProFlex",   "category": "Deporte"},
    {"id": "P003", "name": "Cafetera Espresso DeLux",      "category": "Hogar"},
    {"id": "P004", "name": "Mochila Urbana Trail 30L",     "category": "Accesorios"},
    {"id": "P005", "name": "Smartphone UltraX 12",         "category": "Electrónica"},
    {"id": "P006", "name": "Silla Ergonómica HomeOffice",  "category": "Oficina"},
    {"id": "P007", "name": "Monitor 4K 27'' ProView",      "category": "Electrónica"},
    {"id": "P008", "name": "Bicicleta Estática SmartFit",  "category": "Deporte"},
]

POSITIVE_TEXTS = [
    "Excelente producto, superó todas mis expectativas.",
    "Muy buena calidad, lo recomiendo totalmente.",
    "Entrega rápida y producto tal como se describe.",
    "Llevo meses usándolo y funciona perfecto.",
    "Relación calidad-precio inmejorable.",
    "Estoy muy satisfecho con la compra.",
]

NEUTRAL_TEXTS = [
    "El producto está bien pero nada del otro mundo.",
    "Cumple su función aunque le faltan algunas cosas.",
    "Normal, ni mejor ni peor de lo esperado.",
    "El envío tardó más de lo anunciado.",
    "Le faltan instrucciones en castellano.",
]

NEGATIVE_TEXTS = [
    "Decepcionante, no funciona como se describe.",
    "Mala calidad de materiales, no lo recomiendo.",
    "Se rompió al tercer uso, muy frágil.",
    "El servicio de atención al cliente es pésimo.",
    "No vale el precio que se pide.",
]


def generate_review(fake: Faker) -> dict:
    """Genera una reseña aleatoria con sentimiento correlacionado con el rating."""
    product = random.choice(PRODUCTS)
    rating = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 12, 35, 40])[0]

    if rating >= 4:
        text = random.choice(POSITIVE_TEXTS)
    elif rating == 3:
        text = random.choice(NEUTRAL_TEXTS)
    else:
        text = random.choice(NEGATIVE_TEXTS)

    return {
        "review_id":    str(uuid.uuid4()),
        "product_id":   product["id"],
        "product_name": product["name"],
        "category":     product["category"],
        "user_id":      str(uuid.uuid4()),
        "rating":       rating,
        "review_text":  text,
        "country":      fake.country_code(representation="alpha-2"),
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }


def connect_rabbitmq() -> pika.BlockingConnection:
    """Establece conexión con RabbitMQ con reintentos."""
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


def main():
    fake = Faker("es_ES")
    connection = connect_rabbitmq()
    channel = connection.channel()

    # Cola durable: sobrevive a reinicios del broker
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    log.info("Iniciando publicación de reseñas a '%s' (%s msg/s)…", RABBITMQ_QUEUE, EVENTS_PER_SECOND)

    total = 0
    sleep_time = 1.0 / EVENTS_PER_SECOND

    try:
        while True:
            review = generate_review(fake)
            body = json.dumps(review, ensure_ascii=False)

            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                ),
            )
            total += 1
            if total % 50 == 0:
                log.info("Publicados %d mensajes. Último: %s | rating=%d",
                         total, review["product_name"], review["rating"])
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info("Producción detenida. Total mensajes: %d", total)
    finally:
        connection.close()


if __name__ == "__main__":
    main()

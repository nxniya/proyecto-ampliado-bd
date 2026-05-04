"""
sentiment_analysis.py — Análisis de sentimiento con PySpark + VADER.

Invocado por el DAG de Airflow (task: sentiment_analysis).
Reemplaza el notebook 01 de Azure Databricks.

Flujo:
  1. Descarga el fichero .jsonl desde MinIO (raw-reviews) a /tmp/.
  2. Lee y limpia los datos con PySpark en modo local.
  3. Aplica análisis de sentimiento VADER mediante UDFs de Spark.
  4. Escribe los resultados en PostgreSQL (tabla reviews_sentiment).
  5. Escribe un Parquet en /tmp/ y lo sube al bucket processed-reviews de MinIO.

Uso:
  python sentiment_analysis.py <nombre_fichero.jsonl>
"""

import glob
import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone

import boto3
import psycopg2
import psycopg2.extras
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, IntegerType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SPARK-ETL] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Configuración desde variables de entorno ──────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "raw-reviews")
MINIO_BUCKET_PRO = os.getenv("MINIO_BUCKET_PROCESSED", "processed-reviews")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "reviewsdb")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def main(input_file: str):
    log.info("=== Iniciando análisis de sentimiento para: %s ===", input_file)

    s3 = get_s3_client()
    tmp_dir = tempfile.mkdtemp(prefix="sentimentflow_")

    try:
        # ── 1. Descargar fichero de MinIO ──────────────────────────────────────
        local_input = os.path.join(tmp_dir, input_file)
        log.info("Descargando %s/%s …", MINIO_BUCKET_RAW, input_file)
        s3.download_file(MINIO_BUCKET_RAW, input_file, local_input)
        log.info("Descargado: %.1f KB", os.path.getsize(local_input) / 1024)

        # ── 2. Inicializar Spark en modo local ─────────────────────────────────
        spark = (
            SparkSession.builder
            .appName("SentimentFlow-Analysis")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        log.info("Spark %s iniciado en modo local.", spark.version)

        # ── 3. Lectura y limpieza ──────────────────────────────────────────────
        schema = StructType([
            StructField("review_id",    StringType(),  False),
            StructField("product_id",   StringType(),  False),
            StructField("product_name", StringType(),  True),
            StructField("category",     StringType(),  True),
            StructField("user_id",      StringType(),  True),
            StructField("rating",       IntegerType(), True),
            StructField("review_text",  StringType(),  True),
            StructField("country",      StringType(),  True),
            StructField("timestamp",    StringType(),  True),
        ])

        df_raw = spark.read.schema(schema).json(local_input)
        log.info("Registros leídos: %d", df_raw.count())

        def clean_text(text: str) -> str:
            if not text:
                return ""
            text = text.lower().strip()
            text = re.sub(r"[^\w\s.,!?áéíóúüñ]", " ", text)
            return re.sub(r"\s+", " ", text)

        clean_udf = F.udf(clean_text, StringType())

        df_clean = (
            df_raw
            .withColumn("review_text_clean", clean_udf(F.col("review_text")))
            .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
            .filter(F.col("review_id").isNotNull())
            .filter(F.col("rating").between(1, 5))
            .dropDuplicates(["review_id"])
        )

        # ── 4. Análisis de sentimiento VADER ──────────────────────────────────
        # VADER está entrenado en inglés; los textos en español tienden a
        # puntuaciones neutras. Para mayor precisión se podría usar pysentimiento.
        analyzer = SentimentIntensityAnalyzer()

        def get_compound(text: str) -> float:
            return float(analyzer.polarity_scores(text or "")["compound"])

        def get_label(score: float) -> str:
            if score is None:
                return "Neutro"
            if score >= 0.05:
                return "Positivo"
            if score <= -0.05:
                return "Negativo"
            return "Neutro"

        compound_udf = F.udf(get_compound, DoubleType())
        label_udf    = F.udf(get_label,    StringType())

        df_sentiment = (
            df_clean
            .withColumn("sentiment_score", compound_udf(F.col("review_text_clean")))
            .withColumn("sentiment_label", label_udf(F.col("sentiment_score")))
            .withColumn("processed_at", F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"))
        )

        log.info("Distribución de sentimiento del lote:")
        df_sentiment.groupBy("sentiment_label").count().show()

        # ── 5. Escribir en PostgreSQL ──────────────────────────────────────────
        OUTPUT_COLS = [
            "review_id", "product_id", "product_name", "category",
            "user_id", "rating", "review_text_clean", "country",
            "event_ts", "sentiment_score", "sentiment_label", "processed_at",
        ]
        df_output = df_sentiment.select(*OUTPUT_COLS)
        rows = df_output.collect()

        conn = get_pg_connection()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO reviews_sentiment
                      (review_id, product_id, product_name, category, user_id,
                       rating, review_text, country, event_ts,
                       sentiment_score, sentiment_label, processed_at)
                    VALUES %s
                    ON CONFLICT (review_id) DO NOTHING
                    """,
                    [
                        (
                            r.review_id, r.product_id, r.product_name, r.category,
                            r.user_id, r.rating, r.review_text_clean, r.country,
                            r.event_ts, r.sentiment_score, r.sentiment_label, r.processed_at,
                        )
                        for r in rows
                    ],
                )
            conn.commit()
            log.info("✓ %d filas escritas en PostgreSQL (reviews_sentiment).", len(rows))
        finally:
            conn.close()

        # ── 6. Escribir Parquet y subir a MinIO ───────────────────────────────
        parquet_dir = os.path.join(tmp_dir, "output_parquet")
        df_output.coalesce(1).write.mode("overwrite").parquet(parquet_dir)

        run_date = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        base_name = input_file.replace(".jsonl", "")
        parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))

        for pf in parquet_files:
            object_key = f"{run_date}/{base_name}.parquet"
            s3.upload_file(pf, MINIO_BUCKET_PRO, object_key)
            log.info("✓ Parquet subido a MinIO: processed-reviews/%s", object_key)

        spark.stop()
        log.info("=== Análisis completado: %d reseñas procesadas ===", len(rows))

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python sentiment_analysis.py <nombre_fichero.jsonl>")
        sys.exit(1)
    main(sys.argv[1])

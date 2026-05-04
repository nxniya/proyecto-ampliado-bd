"""
aggregations.py — Cálculo de métricas agregadas con PySpark.

Invocado por el DAG de Airflow (task: aggregations) tras sentiment_analysis.
Reemplaza el notebook 02 de Azure Databricks.

Lee todos los datos de reviews_sentiment de PostgreSQL y actualiza
las tres tablas de agregados que consume el dashboard Streamlit.
"""

import logging
import os

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SPARK-AGG] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "reviewsdb")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def upsert_dataframe(conn, df_pandas, table: str, conflict_col: str, columns: list):
    """Hace upsert de un DataFrame de pandas en una tabla PostgreSQL."""
    if df_pandas.empty:
        return
    rows = [tuple(row[c] for c in columns) for _, row in df_pandas.iterrows()]
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != conflict_col
    )
    sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col}) DO UPDATE SET {update_set}
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def main():
    log.info("=== Iniciando cálculo de agregaciones ===")

    conn = get_pg_connection()
    try:
        # ── 1. Leer datos de PostgreSQL ────────────────────────────────────────
        import pandas as pd
        df_pg = pd.read_sql(
            "SELECT * FROM reviews_sentiment",
            conn,
        )
    finally:
        conn.close()

    if df_pg.empty:
        log.warning("No hay datos en reviews_sentiment. Nada que agregar.")
        return

    log.info("Registros leídos de PostgreSQL: %d", len(df_pg))

    # ── 2. Crear Spark DF a partir del DataFrame de pandas ───────────────────
    spark = (
        SparkSession.builder
        .appName("SentimentFlow-Aggregations")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.createDataFrame(df_pg)

    # ── 3. Agregados por producto ──────────────────────────────────────────────
    df_by_product = df.groupBy("product_id", "product_name", "category").agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
        F.sum(F.when(F.col("sentiment_label") == "Positivo", 1).otherwise(0)).alias("positivos"),
        F.sum(F.when(F.col("sentiment_label") == "Neutro",   1).otherwise(0)).alias("neutros"),
        F.sum(F.when(F.col("sentiment_label") == "Negativo", 1).otherwise(0)).alias("negativos"),
        F.max("processed_at").alias("last_updated"),
    ).toPandas()

    # ── 4. Serie temporal por hora y categoría ─────────────────────────────────
    df_ts = (
        df
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("event_ts")))
        .groupBy("hour_bucket", "category")
        .agg(
            F.count("*").alias("num_reviews"),
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
        )
        .orderBy("hour_bucket")
        .toPandas()
    )

    # ── 5. Sentimiento por país ────────────────────────────────────────────────
    df_country = (
        df.groupBy("country")
        .agg(
            F.count("*").alias("total_reviews"),
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
        )
        .toPandas()
    )

    spark.stop()

    # ── 6. Escribir agregados en PostgreSQL ───────────────────────────────────
    conn = get_pg_connection()
    try:
        upsert_dataframe(
            conn, df_by_product, "agg_by_product", "product_id",
            ["product_id", "product_name", "category", "total_reviews",
             "avg_rating", "avg_sentiment", "positivos", "neutros", "negativos", "last_updated"],
        )
        log.info("✓ agg_by_product actualizada (%d filas).", len(df_by_product))

        # agg_timeseries se reemplaza completamente (sin PK de negocio)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE agg_timeseries")
        conn.commit()
        if not df_ts.empty:
            rows_ts = [
                (row["hour_bucket"], row["category"], int(row["num_reviews"]),
                 row["avg_sentiment"], row["avg_rating"])
                for _, row in df_ts.iterrows()
            ]
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO agg_timeseries (hour_bucket, category, num_reviews, avg_sentiment, avg_rating) VALUES %s",
                    rows_ts,
                )
            conn.commit()
        log.info("✓ agg_timeseries actualizada (%d filas).", len(df_ts))

        upsert_dataframe(
            conn, df_country, "agg_by_country", "country",
            ["country", "total_reviews", "avg_sentiment"],
        )
        log.info("✓ agg_by_country actualizada (%d filas).", len(df_country))

    finally:
        conn.close()

    log.info("=== Agregaciones completadas ===")


if __name__ == "__main__":
    main()

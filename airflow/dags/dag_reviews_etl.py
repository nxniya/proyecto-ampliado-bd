"""
dag_reviews_etl.py — Pipeline ETL de SentimentFlow (reemplaza Azure Data Factory).

Se activa externamente (sin schedule) cuando el consumer sube un fichero
a MinIO y llama a la REST API de Airflow, pasando el nombre del fichero
como parámetro de configuración del DAG run.

Tareas:
  1. sentiment_analysis — Descarga el .jsonl de MinIO, aplica VADER con PySpark
                          y escribe los resultados en PostgreSQL + Parquet en MinIO.
  2. aggregations       — Lee reviews_sentiment de PostgreSQL, calcula métricas
                          agregadas y actualiza las tablas agg_* con PySpark.
"""

import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "sentimentflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
}


def run_sentiment_analysis(**context):
    input_file = context["dag_run"].conf.get("input_file", "")
    if not input_file:
        raise ValueError("El parámetro 'input_file' es obligatorio en dag_run.conf")

    result = subprocess.run(
        [sys.executable, "/opt/spark/sentiment_analysis.py", input_file],
        capture_output=True,
        text=True,
        check=True,
    )
    print(result.stdout)
    if result.stderr:
        print("[STDERR]", result.stderr)


def run_aggregations(**context):
    result = subprocess.run(
        [sys.executable, "/opt/spark/aggregations.py"],
        capture_output=True,
        text=True,
        check=True,
    )
    print(result.stdout)
    if result.stderr:
        print("[STDERR]", result.stderr)


with DAG(
    dag_id="reviews_etl",
    default_args=default_args,
    description="Pipeline ETL: MinIO → PySpark (VADER) → PostgreSQL",
    schedule_interval=None,   # Solo se activa mediante la REST API del consumer
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sentimentflow"],
) as dag:

    sentiment_task = PythonOperator(
        task_id="sentiment_analysis",
        python_callable=run_sentiment_analysis,
    )

    aggregations_task = PythonOperator(
        task_id="aggregations",
        python_callable=run_aggregations,
    )

    sentiment_task >> aggregations_task

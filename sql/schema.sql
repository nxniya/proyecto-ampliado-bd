-- schema.sql — PostgreSQL · SentimentFlow
-- Se ejecuta automáticamente al arrancar el contenedor postgres por primera vez.

-- Tabla principal: una fila por reseña procesada
CREATE TABLE IF NOT EXISTS reviews_sentiment (
    review_id        VARCHAR(36)      NOT NULL PRIMARY KEY,
    product_id       VARCHAR(10)      NOT NULL,
    product_name     VARCHAR(200)     NOT NULL,
    category         VARCHAR(100),
    user_id          VARCHAR(36),
    rating           SMALLINT         NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_text      VARCHAR(2000),
    country          CHAR(2),
    event_ts         TIMESTAMP,
    sentiment_score  FLOAT,
    sentiment_label  VARCHAR(10)      CHECK (sentiment_label IN ('Positivo','Neutro','Negativo')),
    processed_at     TIMESTAMP        NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_reviews_product   ON reviews_sentiment (product_id);
CREATE INDEX IF NOT EXISTS ix_reviews_event_ts  ON reviews_sentiment (event_ts);
CREATE INDEX IF NOT EXISTS ix_reviews_sentiment ON reviews_sentiment (sentiment_label);

-- Agregados por producto (actualizada por el job de agregaciones)
CREATE TABLE IF NOT EXISTS agg_by_product (
    product_id     VARCHAR(10)   NOT NULL PRIMARY KEY,
    product_name   VARCHAR(200)  NOT NULL,
    category       VARCHAR(100),
    total_reviews  INT           NOT NULL DEFAULT 0,
    avg_rating     NUMERIC(4,2),
    avg_sentiment  FLOAT,
    positivos      INT           NOT NULL DEFAULT 0,
    neutros        INT           NOT NULL DEFAULT 0,
    negativos      INT           NOT NULL DEFAULT 0,
    last_updated   TIMESTAMP
);

-- Evolución temporal por hora y categoría
CREATE TABLE IF NOT EXISTS agg_timeseries (
    id             SERIAL        PRIMARY KEY,
    hour_bucket    TIMESTAMP     NOT NULL,
    category       VARCHAR(100)  NOT NULL,
    num_reviews    INT           NOT NULL DEFAULT 0,
    avg_sentiment  FLOAT,
    avg_rating     NUMERIC(4,2)
);

CREATE INDEX IF NOT EXISTS ix_ts_hour ON agg_timeseries (hour_bucket);

-- Sentimiento por país
CREATE TABLE IF NOT EXISTS agg_by_country (
    country        CHAR(2)       NOT NULL PRIMARY KEY,
    total_reviews  INT           NOT NULL DEFAULT 0,
    avg_sentiment  FLOAT
);

-- Auditoría de errores del pipeline Airflow
CREATE TABLE IF NOT EXISTS pipeline_errors (
    id             SERIAL        PRIMARY KEY,
    pipeline_name  VARCHAR(100),
    run_id         VARCHAR(100),
    error_message  TEXT,
    failed_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);

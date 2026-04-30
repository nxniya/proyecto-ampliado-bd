-- ─────────────────────────────────────────────────────────────────────────────
-- schema.sql — Azure SQL Database · SentimentFlow
-- Ejecutar una vez antes de arrancar el pipeline.
-- ─────────────────────────────────────────────────────────────────────────────

-- Tabla principal: una fila por reseña procesada
CREATE TABLE dbo.reviews_sentiment (
    review_id        NVARCHAR(36)     NOT NULL PRIMARY KEY,
    product_id       NVARCHAR(10)     NOT NULL,
    product_name     NVARCHAR(200)    NOT NULL,
    category         NVARCHAR(100)    NULL,
    user_id          NVARCHAR(36)     NULL,
    rating           TINYINT          NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_text      NVARCHAR(2000)   NULL,
    country          NCHAR(2)         NULL,
    event_ts         DATETIME2        NULL,
    sentiment_score  FLOAT            NULL,
    sentiment_label  NVARCHAR(10)     NULL CHECK (sentiment_label IN ('Positivo','Neutro','Negativo')),
    processed_at     DATETIME2        NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX IX_reviews_product   ON dbo.reviews_sentiment (product_id);
CREATE INDEX IX_reviews_event_ts  ON dbo.reviews_sentiment (event_ts);
CREATE INDEX IX_reviews_sentiment ON dbo.reviews_sentiment (sentiment_label);
GO

-- Tabla de agregados por producto (actualizada por notebook 02)
CREATE TABLE dbo.agg_by_product (
    product_id     NVARCHAR(10)  NOT NULL PRIMARY KEY,
    product_name   NVARCHAR(200) NOT NULL,
    category       NVARCHAR(100) NULL,
    total_reviews  INT           NOT NULL DEFAULT 0,
    avg_rating     DECIMAL(4,2)  NULL,
    avg_sentiment  FLOAT         NULL,
    positivos      INT           NOT NULL DEFAULT 0,
    neutros        INT           NOT NULL DEFAULT 0,
    negativos      INT           NOT NULL DEFAULT 0,
    last_updated   DATETIME2     NULL
);
GO

-- Tabla de evolución temporal por hora y categoría
CREATE TABLE dbo.agg_timeseries (
    id             INT           NOT NULL IDENTITY(1,1) PRIMARY KEY,
    hour_bucket    DATETIME2     NOT NULL,
    category       NVARCHAR(100) NOT NULL,
    num_reviews    INT           NOT NULL DEFAULT 0,
    avg_sentiment  FLOAT         NULL,
    avg_rating     DECIMAL(4,2)  NULL
);

CREATE INDEX IX_ts_hour ON dbo.agg_timeseries (hour_bucket);
GO

-- Tabla de sentimiento por país
CREATE TABLE dbo.agg_by_country (
    country        NCHAR(2)  NOT NULL PRIMARY KEY,
    total_reviews  INT       NOT NULL DEFAULT 0,
    avg_sentiment  FLOAT     NULL
);
GO

-- Tabla de auditoría de errores del pipeline ADF
CREATE TABLE dbo.pipeline_errors (
    id            INT           NOT NULL IDENTITY(1,1) PRIMARY KEY,
    pipeline_name NVARCHAR(100) NULL,
    run_id        NVARCHAR(100) NULL,
    error_message NVARCHAR(MAX) NULL,
    failed_at     DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);

CREATE PROCEDURE dbo.sp_log_pipeline_error
    @pipeline_name NVARCHAR(100),
    @run_id        NVARCHAR(100),
    @error_message NVARCHAR(MAX),
    @failed_at     DATETIME2
AS
BEGIN
    INSERT INTO dbo.pipeline_errors (pipeline_name, run_id, error_message, failed_at)
    VALUES (@pipeline_name, @run_id, @error_message, @failed_at);
END;
GO

"""
app.py — Dashboard Streamlit · SentimentFlow
Visualiza el análisis de sentimiento de reseñas almacenadas en PostgreSQL.
"""

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="SentimentFlow Dashboard",
    page_icon="📊",
    layout="wide",
)

# ─── Conexión a PostgreSQL ─────────────────────────────────────────────────────
@st.cache_resource(ttl=60)
def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


@st.cache_data(ttl=30)
def load_by_product() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("SELECT * FROM agg_by_product ORDER BY total_reviews DESC", conn)


@st.cache_data(ttl=30)
def load_timeseries() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("SELECT * FROM agg_timeseries ORDER BY hour_bucket", conn)


@st.cache_data(ttl=30)
def load_by_country() -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql("SELECT * FROM agg_by_country ORDER BY total_reviews DESC", conn)


@st.cache_data(ttl=30)
def load_recent(n: int = 200) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(
        f"""
        SELECT product_name, category, rating,
               sentiment_score, sentiment_label,
               country, event_ts
        FROM reviews_sentiment
        ORDER BY processed_at DESC
        LIMIT {n}
        """,
        conn,
    )


# ─── Layout ───────────────────────────────────────────────────────────────────
st.title("📊 SentimentFlow — Análisis de Sentimiento en Tiempo Real")
st.caption("Pipeline: RabbitMQ → MinIO → Airflow → PySpark (VADER) → PostgreSQL")

try:
    df_product = load_by_product()
    df_ts      = load_timeseries()
    df_country = load_by_country()
    df_recent  = load_recent()
    data_ok = True
except Exception as exc:
    st.error(f"No se pudo conectar a PostgreSQL: {exc}")
    st.info("Asegúrate de que `.env` tiene las credenciales correctas y PostgreSQL está en marcha.")
    data_ok = False

if data_ok:
    # ── KPIs ──────────────────────────────────────────────────────────────────
    total    = int(df_product["total_reviews"].sum()) if not df_product.empty else 0
    avg_sent = float(df_recent["sentiment_score"].mean()) if not df_recent.empty else 0.0
    pct_pos  = (
        int((df_recent["sentiment_label"] == "Positivo").sum()) / len(df_recent) * 100
        if not df_recent.empty else 0.0
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total reseñas procesadas", f"{total:,}")
    col2.metric("Sentimiento medio", f"{avg_sent:+.3f}")
    col3.metric("% Positivas (últimas 200)", f"{pct_pos:.1f}%")
    col4.metric("Productos monitorizados", len(df_product))

    st.divider()

    # ── Fila 1: Barras por producto + Donut sentimiento ───────────────────────
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Reseñas por producto")
        if not df_product.empty:
            fig_bar = px.bar(
                df_product.head(8),
                x="product_name", y="total_reviews",
                color="avg_sentiment",
                color_continuous_scale="RdYlGn",
                range_color=[-1, 1],
                labels={"product_name": "Producto", "total_reviews": "Reseñas",
                        "avg_sentiment": "Sentimiento medio"},
                text="total_reviews",
            )
            fig_bar.update_layout(xaxis_tickangle=-25, coloraxis_showscale=True)
            st.plotly_chart(fig_bar, use_container_width=True)

    with col_right:
        st.subheader("Distribución de sentimiento")
        if not df_recent.empty:
            counts = df_recent["sentiment_label"].value_counts().reset_index()
            counts.columns = ["label", "count"]
            fig_pie = px.pie(
                counts, names="label", values="count",
                color="label",
                color_discrete_map={
                    "Positivo": "#2ecc71",
                    "Neutro":   "#f39c12",
                    "Negativo": "#e74c3c",
                },
                hole=0.4,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    st.divider()

    # ── Fila 2: Serie temporal ─────────────────────────────────────────────────
    st.subheader("Evolución del sentimiento por hora y categoría")
    if not df_ts.empty:
        fig_ts = px.line(
            df_ts,
            x="hour_bucket", y="avg_sentiment",
            color="category",
            labels={"hour_bucket": "Hora", "avg_sentiment": "Sentimiento medio"},
            markers=True,
        )
        fig_ts.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        st.plotly_chart(fig_ts, use_container_width=True)
    else:
        st.info("Aún no hay datos de serie temporal.")

    st.divider()

    # ── Fila 3: Mapa de países + Tabla reciente ────────────────────────────────
    col_map, col_table = st.columns([1, 1])

    with col_map:
        st.subheader("Sentimiento por país")
        if not df_country.empty:
            fig_map = px.choropleth(
                df_country,
                locations="country",
                color="avg_sentiment",
                color_continuous_scale="RdYlGn",
                range_color=[-1, 1],
                labels={"avg_sentiment": "Sentimiento"},
            )
            fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
            st.plotly_chart(fig_map, use_container_width=True)

    with col_table:
        st.subheader("Últimas reseñas procesadas")
        if not df_recent.empty:
            st.dataframe(
                df_recent[["product_name", "rating", "sentiment_label", "sentiment_score", "country"]]
                .rename(columns={
                    "product_name":    "Producto",
                    "rating":          "Rating",
                    "sentiment_label": "Sentimiento",
                    "sentiment_score": "Score",
                    "country":         "País",
                }),
                use_container_width=True,
                height=380,
            )

    st.caption("🔄 Datos actualizados cada 30 segundos. Recarga la página para ver los últimos.")

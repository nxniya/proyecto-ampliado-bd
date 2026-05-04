FROM apache/airflow:2.9.1

USER root
# Java es necesario para PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    vaderSentiment \
    boto3 \
    psycopg2-binary

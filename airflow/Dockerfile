FROM apache/airflow:2.8.1-python3.11

USER root

# Install OpenJDK-17 and procps (Critical: 'ps' command is required by Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        ant \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install PySpark and Providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4 \
    pyspark==3.5.0

# Set SPARK_HOME to the location where pip installs it
# This ensures the SparkSubmitOperator finds the correct configuration
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin


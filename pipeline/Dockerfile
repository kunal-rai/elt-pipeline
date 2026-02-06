# pipeline/Dockerfile
FROM python:3.11-slim

# Install Java 21 (required for PySpark on Debian trixie / ARM64)
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        wget \
        ca-certificates \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for pyspark (Java 21)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Download PostgreSQL JDBC driver
RUN mkdir -p /opt/jdbc && \
    wget -q -O /opt/jdbc/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Copy application code
COPY app ./app

ENV PYTHONUNBUFFERED=1

CMD ["python","app/scheduler.py"]

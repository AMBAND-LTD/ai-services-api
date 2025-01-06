# Builder Stage
FROM python:3.11-slim AS builder

# Install System Dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    build-essential \
    libpq-dev \
    gcc \
    python3-dev \
    postgresql-client \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
    libglib2.0-0 \
    libx11-6 \
    && rm -rf /var/lib/apt/lists/*

# Install Chromium and ChromeDriver
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Fix tmp permissions
RUN chmod 1777 /tmp

# Poetry Installation
RUN pip install --upgrade pip && \
    pip install poetry && \
    poetry config virtualenvs.create false

# Project Dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry install --with dev --no-root

# Additional Python Packages
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch && \
    pip install sentence-transformers && \
    pip install faiss-cpu==1.9.0.post1 && \
    pip install \
        apache-airflow==2.7.3 \
        apache-airflow-providers-celery==3.3.1 \
        apache-airflow-providers-postgres==5.6.0 \
        apache-airflow-providers-redis==3.3.1 \
        apache-airflow-providers-http==4.1.0 \
        apache-airflow-providers-common-sql==1.10.0 \
        croniter==2.0.1 \
        cryptography==42.0.0

# Final Stage
FROM python:3.11-slim

# Install System Dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libomp-dev \
    curl \
    wget \
    unzip \
    redis-tools \
    netcat-openbsd \
    chromium \
    chromium-driver \
    libglib2.0-0 \
    libnss3 \
    libx11-6 \
    && rm -rf /var/lib/apt/lists/*

# User and Group Setup with non-unique IDs
RUN groupadd --non-unique -g 125 appgroup && \
    useradd --non-unique -u 1001 -g appgroup -s /bin/bash -m appuser && \
    usermod -aG root,appgroup appuser

# Chrome directories and permissions setup
RUN mkdir -p /tmp/chrome-data /var/run/chrome && \
    chown -R 1001:125 /tmp/chrome-data /var/run/chrome && \
    chmod -R 1777 /tmp/chrome-data /var/run/chrome

# Fix permissions
RUN chmod 1777 /tmp && \
    chown -R 1001:125 /tmp

# Chrome sandbox setup
RUN chown root:root /usr/bin/chromium && \
    chmod 4755 /usr/bin/chromium

# Directory Structure
RUN mkdir -p \
    /code/ai_services_api/services/search/models \
    /code/logs \
    /code/cache \
    /opt/airflow/logs \
    /opt/airflow/dags \
    /opt/airflow/plugins \
    /opt/airflow/data \
    /code/scripts \
    /code/tests

# Permissions for application directories
RUN chown -R 1001:125 /code && \
    chown -R 1001:125 /opt/airflow && \
    chmod -R 775 /code && \
    chmod -R 775 /opt/airflow

# Working Directory
WORKDIR /code

# Copy Dependencies
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Application Files
COPY --chown=1001:125 . .
RUN chmod +x /code/scripts/init-script.sh

# Set Chrome flags
ENV CHROME_FLAGS="--headless=new --no-sandbox --disable-gpu --disable-dev-shm-usage --disable-crashpad --disable-crash-reporter --disable-software-rasterizer --remote-debugging-port=9222"

# Environment Variables
ENV TRANSFORMERS_CACHE=/code/cache \
    HF_HOME=/code/cache \
    AIRFLOW_HOME=/opt/airflow \
    PYTHONPATH=/code \
    TESTING=false \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    CHROME_TMPDIR=/tmp/chrome-data

# Health Check
HEALTHCHECK --interval=30s \
            --timeout=10s \
            --start-period=60s \
            --retries=3 \
            CMD curl -f http://localhost:8000/health || exit 1

# User Switch
USER 1001:125

# Default Command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
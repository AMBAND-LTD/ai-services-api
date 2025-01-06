# ===============================
# Builder Stage
# ===============================
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
    && rm -rf /var/lib/apt/lists/*

# Install Chromium
RUN apt-get update && apt-get install -y chromium \
    && rm -rf /var/lib/apt/lists/*

# Fetch the Latest ChromeDriver Version
RUN export CHROMEDRIVER_VERSION=$(curl -sS https://chromedriver.storage.googleapis.com/LATEST_RELEASE) \
    && wget -q https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip \
    && mv chromedriver /usr/bin/chromedriver \
    && chmod +x /usr/bin/chromedriver \
    && rm chromedriver_linux64.zip

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

# ===============================
# Final Stage
# ===============================
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
    && rm -rf /var/lib/apt/lists/*

# Install Chromium
RUN apt-get update && apt-get install -y chromium \
    && rm -rf /var/lib/apt/lists/*

# Fetch and Install ChromeDriver
RUN export CHROMEDRIVER_VERSION=$(curl -sS https://chromedriver.storage.googleapis.com/LATEST_RELEASE) \
    && wget -q https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip \
    && unzip chromedriver_linux64.zip \
    && mv chromedriver /usr/bin/chromedriver \
    && chmod +x /usr/bin/chromedriver \
    && rm chromedriver_linux64.zip

# User and Group Setup
RUN groupadd -g 1001 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/bash -m appuser

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

# Permissions
RUN chown -R appuser:appgroup /code && \
    chown -R appuser:appgroup /opt/airflow && \
    chmod -R 775 /code && \
    chmod -R 775 /opt/airflow

# Working Directory
WORKDIR /code

# Copy Dependencies
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Application Files
COPY --chown=appuser:appgroup . . 
RUN chmod +x /code/scripts/init-script.sh

# Set Chrome flags
ENV CHROME_FLAGS="--headless --no-sandbox --disable-dev-shm-usage --remote-debugging-pipe --disable-extensions"

# Environment Variables
ENV TRANSFORMERS_CACHE=/code/cache \
    HF_HOME=/code/cache \
    AIRFLOW_HOME=/opt/airflow \
    PYTHONPATH=/code \
    TESTING=false \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Health Check
HEALTHCHECK --interval=30s \
            --timeout=10s \
            --start-period=60s \
            --retries=3 \
            CMD curl -f http://localhost:8000/health || exit 1

# User Switch
USER appuser

# Default Command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

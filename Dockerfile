# =============================== 
# Builder Stage 
# =============================== 
FROM python:3.11-slim as builder

# System Dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    libpq-dev \
    gcc \
    python3-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Poetry Installation
RUN pip install --upgrade pip && \
    pip install poetry && \
    poetry config virtualenvs.create false

# Project Dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry install --with dev

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
        cryptography==42.0.0 \
        selenium==4.15.2 \
        webdriver_manager==4.0.1

# =============================== 
# Final Stage 
# =============================== 
FROM python:3.11-slim

# System Dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libomp-dev \
    curl \
    redis-tools \
    netcat-openbsd \
    wget \
    chromium \
    chromium-driver \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

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

# Environment Variables
ENV TRANSFORMERS_CACHE=/code/cache \
    HF_HOME=/code/cache \
    AIRFLOW_HOME=/opt/airflow \
    PYTHONPATH=/code \
    TESTING=false \
    DISPLAY=:99 \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Xvfb setup
RUN echo 'Xvfb :99 -screen 0 1024x768x16 &' > /code/scripts/start-xvfb.sh && \
    chmod +x /code/scripts/start-xvfb.sh

# Health Check
HEALTHCHECK --interval=30s \
            --timeout=10s \
            --start-period=60s \
            --retries=3 \
            CMD curl -f http://localhost:8000/health || exit 1

# User Switch
USER appuser

# Default Command
CMD ["/bin/bash", "-c", "/code/scripts/start-xvfb.sh && uvicorn main:app --host 0.0.0.0 --port 8000 --reload"]
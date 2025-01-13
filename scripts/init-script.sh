#!/bin/bash
set -e

# Environment setup
INIT_MARKER="/code/.initialization_complete/init.done"
LOG_DIR="/code/logs"
mkdir -p "$LOG_DIR" "/code/.initialization_complete"

# Logging setup
exec 1> >(tee -a "${LOG_DIR}/init.log") 2>&1
echo "[$(date)] Starting initialization..."

# Function to check services
check_service() {
    local service=$1
    local host=$2
    local port=$3
    local max_attempts=$4
    local attempt=0
    
    echo "[$(date)] Checking $service..."
    until nc -z "$host" "$port" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ $attempt -eq $max_attempts ]; then
            echo "[$(date)] ERROR: $service not available after $max_attempts attempts"
            return 1
        fi
        echo "[$(date)] Waiting for $service... (attempt $attempt/$max_attempts)"
        sleep 5
    done
    echo "[$(date)] $service is available"
}

# Function to verify database tables
verify_database() {
    echo "[$(date)] Verifying database tables..."
    if ! python -c "
from ai_services_api.services.centralized_repository.database_setup import get_db_connection
conn = get_db_connection()
cur = conn.cursor()
try:
    cur.execute(\"\"\"
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'resources_resource'
        );
    \"\"\")
    exists = cur.fetchone()[0]
    if not exists:
        exit(1)
finally:
    cur.close()
    conn.close()
"; then
        echo "[$(date)] Required database tables not found"
        return 1
    fi
    echo "[$(date)] Database verification successful"
    return 0
}

# Function to initialize app
initialize_app() {
    if [ -f "$INIT_MARKER" ]; then
        echo "[$(date)] System initialization marker found"
        if verify_database; then
            echo "[$(date)] System already properly initialized"
            return 0
        else
            echo "[$(date)] Database verification failed, reinitializing..."
            rm "$INIT_MARKER"
        fi
    fi
    
    # Check dependencies with increased timeout
    check_service "PostgreSQL" "postgres" 5432 60
    check_service "Redis" "redis" 6379 30
    check_service "Neo4j" "neo4j" 7687 30
    
    # Build command arguments based on environment variables
    SETUP_ARGS=""
    [ "${SKIP_OPENALEX:-false}" = "true" ] && SETUP_ARGS="$SETUP_ARGS --skip-openalex"
    [ "${SKIP_PUBLICATIONS:-false}" = "true" ] && SETUP_ARGS="$SETUP_ARGS --skip-publications"
    [ "${SKIP_GRAPH:-false}" = "true" ] && SETUP_ARGS="$SETUP_ARGS --skip-graph"
    [ "${SKIP_SEARCH:-false}" = "true" ] && SETUP_ARGS="$SETUP_ARGS --skip-search"
    [ "${SKIP_REDIS:-false}" = "true" ] && SETUP_ARGS="$SETUP_ARGS --skip-redis"
    
    echo "[$(date)] Running database setup..."
    if ! python -m setup $SETUP_ARGS; then
        echo "[$(date)] Database setup failed"
        return 1
    fi

    # Verify database setup was successful
    if ! verify_database; then
        echo "[$(date)] Database verification failed after setup"
        return 1
    fi
    
    # Only create search index if not skipped
    if [ "${SKIP_SEARCH:-false}" != "true" ]; then
        echo "[$(date)] Creating search index..."
        if ! python -m ai_services_api.services.search.index_creator; then
            echo "[$(date)] Search index creation failed"
            return 1
        fi
    fi
    
    # Only create Redis embeddings if not skipped
    if [ "${SKIP_REDIS:-false}" != "true" ]; then
        echo "[$(date)] Creating Redis embeddings..."
        if ! python -m ai_services_api.services.search.redis_embeddings; then
            echo "[$(date)] Redis embeddings creation failed"
            return 1
        fi
    fi

    # Create initialization marker only after successful setup
    date > "$INIT_MARKER"
    echo "[$(date)] Initialization complete"
    return 0
}

# Main execution
if initialize_app; then
    echo "[$(date)] Waiting for systems to be ready..."
    sleep 5  # Add a small delay to ensure all systems are ready
    
    echo "[$(date)] Performing final database verification..."
    if verify_database; then
        echo "[$(date)] Starting application..."
        exec uvicorn ai_services_api.main:app --host 0.0.0.0 --port 8000 --reload
    else
        echo "[$(date)] Final database verification failed!"
        exit 1
    fi
else
    echo "[$(date)] Initialization failed!"
    exit 1
fi

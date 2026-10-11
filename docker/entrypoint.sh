#!/bin/bash
# filepath: docker/entrypoint.sh
set -eo pipefail

echo "=========================================================="
echo " Starting Recommendation Serving Gateway Entrypoint..."
echo "=========================================================="

DB_HOST="${POSTGRES_HOST:-db}"
DB_PORT="${POSTGRES_PORT:-5432}"
REDIS_TARGET_HOST="${REDIS_HOST:-redis}"
REDIS_TARGET_PORT="${REDIS_PORT:-6379}"

# 1. Wait for PostgreSQL
echo "[INFO] Verifying PostgreSQL connection at ${DB_HOST}:${DB_PORT}..."
until python -c "
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(2)
try:
    s.connect(('${DB_HOST}', int('${DB_PORT}')))
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" > /dev/null 2>&1; do
    echo "[WAIT] PostgreSQL not ready yet. Retrying in 1s..."
    sleep 1
done
echo "[SUCCESS] PostgreSQL connection established."

# 2. Wait for Redis if configured
if [ -n "${REDIS_TARGET_HOST}" ]; then
    echo "[INFO] Verifying Redis Feature Store at ${REDIS_TARGET_HOST}:${REDIS_TARGET_PORT}..."
    until python -c "
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(2)
try:
    s.connect(('${REDIS_TARGET_HOST}', int('${REDIS_TARGET_PORT}')))
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" > /dev/null 2>&1; do
        echo "[WAIT] Redis not ready yet. Retrying in 1s..."
        sleep 1
    done
    echo "[SUCCESS] Redis connection established."
fi

# 3. Launch FastAPI Serving Gateway
echo "[INFO] Starting FastAPI application (backend.app.main:app)..."
exec uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --workers 2
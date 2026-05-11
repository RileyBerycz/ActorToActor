#!/bin/bash
set -e

echo "Starting database update in background (logs: /app/data/update.log)..."
python actor_service.py update 20 > /app/data/update.log 2>&1 &

echo "Starting API server..."
exec python api_server.py

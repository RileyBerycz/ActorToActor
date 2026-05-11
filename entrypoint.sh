#!/bin/bash
set -e

echo "Starting database update in background..."
python actor_service.py update 20 &

echo "Starting API server..."
exec python api_server.py

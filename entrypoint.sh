#!/bin/bash
set -e

# Only seed if DB is empty
if [ ! -f /app/data/actors.db ] || [ ! -s /app/data/actors.db ]; then
    echo "Starting database update in background (logs: /app/data/update.log)..."
    python actor_service.py update 100 > /app/data/update.log 2>&1 &
else
    ACTOR_COUNT=$(python3 -c "import sqlite3; conn=sqlite3.connect('/app/data/actors.db'); print(conn.execute('SELECT COUNT(*) FROM actors').fetchone()[0]); conn.close()" 2>/dev/null || echo "0")
    if [ "$ACTOR_COUNT" -lt 100 ]; then
        echo "Starting database update in background (logs: /app/data/update.log)..."
        python actor_service.py update 100 > /app/data/update.log 2>&1 &
    else
        echo "Database already has $ACTOR_COUNT actors, skipping update"
    fi
fi

echo "Starting API server..."
exec python api_server.py

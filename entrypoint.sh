#!/bin/bash
set -e

# Seed DB if it doesn't have enough actors
ACTOR_COUNT=0
if [ -f /app/data/actors.db ] && [ -s /app/data/actors.db ]; then
    ACTOR_COUNT=$(python3 -c "
import sqlite3
try:
    conn = sqlite3.connect('/app/data/actors.db')
    c = conn.execute('SELECT COUNT(*) FROM actors')
    print(c.fetchone()[0])
    conn.close()
except:
    print(0)
" 2>/dev/null || echo "0")
fi

if [ "$ACTOR_COUNT" -lt 2000 ]; then
    echo "Database has $ACTOR_COUNT actors, seeding with 200 pages (logs: /app/data/update.log)..."
    python actor_service.py update 200 > /app/data/update.log 2>&1 &
    SEED_PID=$!
    echo "Seed started (PID: $SEED_PID)"
else
    echo "Database has $ACTOR_COUNT actors, skipping seed"
fi

# Start scheduler for ongoing updates (runs daily at 2 AM + hourly)
echo "Starting scheduler..."
SCHEDULER_PID=""
if python3 -c "import schedule" 2>/dev/null; then
    nohup python3 -c "
import schedule, time, subprocess, logging, os
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('scheduler')

def daily():
    log.info('Running daily 50-page update...')
    subprocess.run(['python', 'actor_service.py', 'update', '50'],
        capture_output=True, timeout=7200)

schedule.every().day.at('02:00').do(daily)

log.info('Scheduler running (daily at 2 AM)')
while True:
    schedule.run_pending()
    time.sleep(60)
" > /app/data/scheduler.log 2>&1 &
    SCHEDULER_PID=$!
    echo "Scheduler started (PID: $SCHEDULER_PID)"
fi

echo "Starting API server..."
exec python api_server.py

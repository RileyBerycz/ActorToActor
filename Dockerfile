# Stage 1: Build React frontend
FROM node:18-alpine AS frontend-builder
WORKDIR /build
COPY actor-game/package*.json ./
RUN npm ci
COPY actor-game/ ./
RUN npm run build

# Stage 2: Python backend + React frontend
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY actor_service.py api_server.py scheduler.py entrypoint.sh ./
COPY --from=frontend-builder /build/build ./actor-game/build/

RUN mkdir -p /app/data && chmod +x entrypoint.sh

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:5000/health || exit 1

ENTRYPOINT ["./entrypoint.sh"]
# Container for the Sticky.io -> BigQuery ETL, running as a Cloud Run SERVICE
# (a web server). Cloud Run sends requests to it; you trigger runs via /run.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/

# Cloud Run provides the PORT environment variable (usually 8080).
# The web server must listen on 0.0.0.0:$PORT.
CMD ["/bin/sh", "-c", "uvicorn src.server:app --host 0.0.0.0 --port ${PORT:-8080}"]

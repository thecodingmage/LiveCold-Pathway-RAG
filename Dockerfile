FROM python:3.11-slim

WORKDIR /app

# Install system deps needed by sentence-transformers
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements-slim.txt .
RUN pip install --no-cache-dir -r requirements-slim.txt

# Copy application code
COPY main.py .
COPY core/ core/
COPY scripts/ scripts/
COPY dashboard/ dashboard/
COPY pipeline/ pipeline/
COPY decision_engine/ decision_engine/
COPY sim/ sim/
COPY watched_docs/ watched_docs/

# Create data directories
RUN mkdir -p demo_data metrics

# Expose ports: dashboard + Pathway RAG
EXPOSE 5050 8765

# Entrypoint
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]

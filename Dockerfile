FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MARKETFORGE_DATABASE=/app/deploy/generated/marketforge-demo.duckdb \
    MARKETFORGE_METADATA_STORE=/app/deploy/generated/operational.sqlite \
    MARKETFORGE_LINEAGE=/app/deploy/generated/lineage.json \
    MARKETFORGE_RAW_ROOT=/app/deploy/generated/raw \
    MARKETFORGE_BENCHMARKS=/app/deploy/generated/benchmarks.json \
    MARKETFORGE_BUDGET=/app/config/resource_budget.yaml \
    MARKETFORGE_PROJECT_ROOT=/app

WORKDIR /app

COPY requirements-demo.txt ./
RUN pip install --no-cache-dir -r requirements-demo.txt

COPY backend ./backend
COPY ingestion ./ingestion
COPY observability ./observability
COPY warehouse ./warehouse
COPY scripts ./scripts
COPY config ./config

RUN python -m scripts.build_demo_snapshot \
    && addgroup --system marketforge \
    && adduser --system --ingroup marketforge --home /nonexistent marketforge \
    && chown -R marketforge:marketforge /app

USER marketforge
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=3).read()"

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]

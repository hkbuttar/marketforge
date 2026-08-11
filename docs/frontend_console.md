# Data platform console

The React console is an internal control-plane interface, with eight areas:
Overview, Datasets, Pipeline Runs, Data Quality, Lineage, Analytics, Benchmarks,
and System.
It consumes only the bounded FastAPI routes and contains no direct warehouse or
raw-file access.

```bash
cd frontend
npm install
npm run dev
```

Copy `.env.example` to `.env` when the API is on another origin. The default uses
the frontend origin, which is appropriate behind a reverse proxy. Vite development
can use `VITE_API_URL=http://127.0.0.1:8000`.
The backend permits the two local Vite origins by default; production origins must
be supplied explicitly with `MARKETFORGE_CORS_ORIGINS`.

The interface treats missing marts or metadata as operational state: it shows the
backend error and empty-state guidance rather than sample market data. Pipeline
Runs and quality evidence use bounded API responses; the browser never reads
SQLite, DuckDB, or Parquet directly. Benchmarks label machine-specific evidence,
storage shows configured budget use, and Analytics remains one consumer section
rather than the main product identity.

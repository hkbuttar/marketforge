# Data platform console

The React console is an internal control-plane interface, with seven areas:
Overview, Datasets, Lineage, Pipeline Runs, Data Quality, Analytics, and System.
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

The interface treats missing marts as an operational state: it shows the backend
error and empty-state guidance rather than sample market data. Pipeline Runs is
also explicit about its pending bounded API instead of reading SQLite directly.
Analytics is deliberately one section of the control plane rather than the main
product identity.

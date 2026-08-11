"""FastAPI application serving only approved MarketForge marts."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request

from backend.schemas import (
    DatasetHealthList, DatasetList, DatasetSummary, LineageResponse,
    LivenessResponse, ReadinessResponse, SecurityDetail, SecurityHistory, SecurityList,
)
from backend.services.health import readiness
from backend.services.query import MartUnavailable, QueryService


def create_app(*, database: Path | None = None, lineage_path: Path | None = None,
               metadata_store: Path | None = None) -> FastAPI:
    database = database or Path(os.getenv("MARKETFORGE_DATABASE", "warehouse/duckdb/marketforge.duckdb"))
    lineage_path = lineage_path or Path(os.getenv("MARKETFORGE_LINEAGE", "warehouse/metadata/lineage.json"))
    metadata_store = metadata_store or Path(os.getenv(
        "MARKETFORGE_METADATA_STORE", "warehouse/metadata/operational.sqlite"))
    app = FastAPI(title="MarketForge API", version="0.1.0")
    app.state.query_service = QueryService(database, lineage_path)

    @app.get("/health/live", response_model=LivenessResponse)
    def live():
        return {"status": "alive", "checked_at": datetime.now(timezone.utc)}

    @app.get("/health/ready", response_model=ReadinessResponse)
    def ready(request: Request):
        checks = readiness(database, metadata_store)
        is_ready = all(check["status"] == "ready" for check in checks)
        body = {"status": "ready" if is_ready else "not_ready",
                "checked_at": datetime.now(timezone.utc), "checks": checks,
                "cache": request.app.state.query_service.cache_stats()}
        if not is_ready:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=503, content=ReadinessResponse(**body).model_dump(mode="json"))
        return body

    def service(request: Request) -> QueryService:
        return request.app.state.query_service

    @app.exception_handler(MartUnavailable)
    async def unavailable(_request: Request, exc: MartUnavailable):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=503, content={"detail": str(exc)})

    @app.get("/api/securities", response_model=SecurityList)
    def securities(limit: int = Query(100, ge=1, le=500), query: QueryService = Depends(service)):
        rows = query.securities(limit)
        return {"data": rows, "meta": {"limit": limit, "returned": len(rows)}}

    @app.get("/api/securities/{symbol}", response_model=SecurityDetail)
    def security(symbol: str, query: QueryService = Depends(service)):
        row = query.security(symbol)
        if row is None:
            raise HTTPException(404, "security not found")
        return row

    @app.get("/api/securities/{symbol}/history", response_model=SecurityHistory)
    def history(symbol: str, source: str | None = None,
                limit: int = Query(252, ge=1, le=2000), query: QueryService = Depends(service)):
        rows = query.history(symbol, source, limit)
        if not rows:
            raise HTTPException(404, "security history not found")
        return {"symbol": symbol.upper(), "source": source, "data": rows,
                "meta": {"limit": limit, "returned": len(rows)}}

    @app.get("/api/pipeline/health", response_model=DatasetHealthList)
    def health(query: QueryService = Depends(service)):
        rows = query.health()
        return {"data": rows, "meta": {"limit": 100, "returned": len(rows)}}

    @app.get("/api/datasets", response_model=DatasetList)
    def datasets(query: QueryService = Depends(service)):
        rows = query.health()
        data = [DatasetSummary(dataset=row["dataset"], status=row["status"], row_count=row["row_count"])
                for row in rows]
        return {"data": data, "meta": {"limit": 100, "returned": len(data)}}

    @app.get("/api/datasets/{dataset}/lineage", response_model=LineageResponse)
    def lineage(dataset: str, query: QueryService = Depends(service)):
        try:
            graph = query.lineage(dataset.lower())
        except KeyError:
            raise HTTPException(404, "dataset lineage not found") from None
        return {"dataset": dataset.lower(), "nodes": graph["nodes"], "edges": graph["edges"],
                "generated_at": graph["generated_at"]}

    return app


app = create_app()

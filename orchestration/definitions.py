"""MarketForge asset graph and local jobs."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import duckdb
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetSelection,
    ConfigurableResource,
    Definitions,
    DefaultScheduleStatus,
    Failure,
    FreshnessPolicy,
    MaterializeResult,
    Output,
    RunRequest,
    RetryPolicy,
    SkipReason,
    asset,
    define_asset_job,
    multi_asset,
    schedule,
)

from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_backfill, run_incremental
from ingestion.sources.files import read_records


DATASETS = ("prices", "fundamentals", "earnings", "macro", "news")
FRESHNESS_WINDOWS = {
    "prices": (timedelta(hours=48), timedelta(hours=36)),
    "fundamentals": (timedelta(days=120), timedelta(days=90)),
    "earnings": (timedelta(days=14), timedelta(days=7)),
    "macro": (timedelta(days=45), timedelta(days=30)),
    "news": (timedelta(hours=48), timedelta(hours=36)),
}


class PlatformResource(ConfigurableResource):
    raw_root: str = "data/raw"
    quarantine_root: str = "data/quarantine"
    metadata_root: str = "warehouse/metadata/ingestion_runs"
    checkpoint_db: str = "warehouse/metadata/checkpoints.sqlite"
    dbt_project_dir: str = "dbt"
    dbt_profiles_dir: str = "dbt"
    dbt_executable: str = ".venv/bin/dbt"
    source: str = "configured-provider"
    mode: Literal["observe", "incremental", "backfill"] = "observe"
    initial_start: str | None = None
    overlap_days: int = 0
    prices_input: str = ""
    fundamentals_input: str = ""
    earnings_input: str = ""
    macro_input: str = ""
    news_input: str = ""


def _manifest(resource: PlatformResource, dataset: str) -> dict:
    manifests = []
    for path in Path(resource.metadata_root).glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("dataset") == dataset and payload.get("status") in {"success", "degraded"}:
            manifests.append(payload)
    if not manifests:
        raise Failure(f"no successful ingestion manifest exists for {dataset}")
    return max(manifests, key=lambda item: (item["completed_at"], item["run_id"]))


def _raw_asset(context: AssetExecutionContext, resource: PlatformResource, dataset: str):
    location = getattr(resource, f"{dataset}_input")
    if resource.mode != "observe":
        if not location:
            raise Failure(f"{dataset}_input is required in {resource.mode} mode")
        records = read_records(location)
        options = {
            "source": resource.source,
            "raw_root": Path(resource.raw_root),
            "quarantine_root": Path(resource.quarantine_root),
            "metadata_root": Path(resource.metadata_root),
        }
        if resource.mode == "backfill":
            result = run_backfill(dataset, records, **options)
        else:
            result = run_incremental(
                dataset,
                records,
                checkpoint_store=CheckpointStore(Path(resource.checkpoint_db)),
                initial_start=date.fromisoformat(resource.initial_start) if resource.initial_start else None,
                overlap_days=resource.overlap_days,
                **options,
            ).backfill
        context.log.info("Ingestion result: %s", asdict(result))
    manifest = _manifest(resource, dataset)
    files = list((Path(resource.raw_root) / dataset).glob("year=*/month=*/*.parquet"))
    if not files:
        raise Failure(f"successful manifest exists but no final Parquet files exist for {dataset}")
    with duckdb.connect() as connection:
        row_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(Path(resource.raw_root) / dataset / "year=*/month=*/*.parquet")],
        ).fetchone()[0]
    return MaterializeResult(
        metadata={
            "run_id": manifest["run_id"], "rows": row_count, "files": len(files),
            "latest_event_date": manifest.get("max_event_date") or "unknown",
            "late_arriving_rows": manifest.get("late_arriving_rows", 0),
            "earliest_late_event_date": manifest.get("earliest_late_event_date") or "none",
            "contract_version": manifest.get("contract_version", "unknown"),
            "ingestion_status": manifest["status"],
        }
    )


def raw_asset(dataset: str):
    fail_window, warn_window = FRESHNESS_WINDOWS[dataset]

    @asset(
        name=f"raw_{dataset}", group_name="raw", compute_kind="python",
        freshness_policy=FreshnessPolicy.time_window(
            fail_window=fail_window, warn_window=warn_window
        ),
        retry_policy=RetryPolicy(max_retries=2, delay=5),
    )
    def generated(context, platform: PlatformResource):
        return _raw_asset(context, platform, dataset)
    return generated


raw_prices, raw_fundamentals, raw_earnings, raw_macro, raw_news = (
    raw_asset(dataset) for dataset in DATASETS
)


def _run_dbt(context: AssetExecutionContext, platform: PlatformResource, arguments: list[str]) -> None:
    command = [
        platform.dbt_executable, *arguments,
        "--project-dir", platform.dbt_project_dir,
        "--profiles-dir", platform.dbt_profiles_dir,
        "--vars", json.dumps({
            "raw_root": platform.raw_root,
            "metadata_root": platform.metadata_root,
        }),
        "--no-use-colors",
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    context.log.info(completed.stdout)
    if completed.returncode:
        raise Failure(f"dbt command failed ({completed.returncode}): {completed.stderr}")


STAGING_OUTS = {f"stg_{dataset}": AssetOut(group_name="staging") for dataset in DATASETS}
STAGING_DEPS = {
    f"stg_{dataset}": {AssetKey(f"raw_{dataset}")} for dataset in DATASETS
}


@multi_asset(
    outs=STAGING_OUTS, internal_asset_deps=STAGING_DEPS,
    deps=[AssetKey(f"raw_{dataset}") for dataset in DATASETS], compute_kind="dbt",
)
def staging_models(context, platform: PlatformResource):
    _run_dbt(context, platform, ["build", "--select", "path:models/staging"])
    for dataset in DATASETS:
        yield Output(None, output_name=f"stg_{dataset}")


INTERMEDIATE_MODELS = (
    "int_daily_returns", "int_rolling_volatility", "int_security_fundamentals",
    "int_earnings_surprises", "int_macro_aligned",
)
INTERMEDIATE_DEPS = {
    "int_daily_returns": {AssetKey("stg_prices")},
    "int_rolling_volatility": {AssetKey("int_daily_returns")},
    "int_security_fundamentals": {AssetKey("stg_fundamentals")},
    "int_earnings_surprises": {AssetKey("stg_earnings")},
    "int_macro_aligned": {AssetKey("stg_prices"), AssetKey("stg_macro")},
}


@multi_asset(
    outs={name: AssetOut(group_name="intermediate") for name in INTERMEDIATE_MODELS},
    internal_asset_deps=INTERMEDIATE_DEPS,
    deps=[AssetKey(name) for name in ("stg_prices", "stg_fundamentals", "stg_earnings", "stg_macro")],
    compute_kind="dbt",
)
def intermediate_models(context, platform: PlatformResource):
    _run_dbt(context, platform, ["build", "--select", "path:models/intermediate"])
    for name in INTERMEDIATE_MODELS:
        yield Output(None, output_name=name)


MART_MODELS = (
    "mart_security_daily", "mart_market_daily", "mart_company_snapshot",
    "mart_pipeline_dataset_health",
)
MART_DEPS = {
    "mart_security_daily": {AssetKey("int_rolling_volatility")},
    "mart_market_daily": {AssetKey("int_daily_returns")},
    "mart_company_snapshot": {
        AssetKey("stg_prices"), AssetKey("int_security_fundamentals"), AssetKey("int_earnings_surprises")
    },
    "mart_pipeline_dataset_health": {AssetKey(f"stg_{dataset}") for dataset in DATASETS},
}


@multi_asset(
    outs={name: AssetOut(group_name="marts") for name in MART_MODELS},
    internal_asset_deps=MART_DEPS,
    deps=list(set().union(*MART_DEPS.values())),
    compute_kind="dbt",
)
def mart_models(context, platform: PlatformResource):
    _run_dbt(context, platform, ["build", "--select", "path:models/marts"])
    for name in MART_MODELS:
        yield Output(None, output_name=name)


@asset(deps=[AssetKey(name) for name in MART_MODELS], group_name="quality", compute_kind="dbt")
def quality_gate(context, platform: PlatformResource):
    _run_dbt(context, platform, ["test"])
    _run_dbt(context, platform, ["source", "freshness"])
    return MaterializeResult(metadata={"gate": "passed"})


@asset(
    deps=[AssetKey("quality_gate"), AssetKey("mart_security_daily"),
          AssetKey("mart_company_snapshot"), AssetKey("mart_pipeline_dataset_health")],
    group_name="serving",
)
def api_ready():
    return MaterializeResult(metadata={"publishable": True})


daily_incremental = define_asset_job("daily_incremental")
historical_backfill = define_asset_job("historical_backfill")
quality_validation_job = define_asset_job(
    "quality_validation", selection="quality_gate+"
)
rebuild_marts = define_asset_job(
    "rebuild_marts",
    selection=AssetSelection.assets(*MART_MODELS, "quality_gate", "api_ready"),
)

prices_ingestion = define_asset_job(
    "prices_ingestion", selection=AssetSelection.assets("raw_prices")
)
macro_ingestion = define_asset_job(
    "macro_ingestion", selection=AssetSelection.assets("raw_macro")
)
fundamentals_ingestion = define_asset_job(
    "fundamentals_ingestion", selection=AssetSelection.assets("raw_fundamentals")
)
earnings_ingestion = define_asset_job(
    "earnings_ingestion", selection=AssetSelection.assets("raw_earnings")
)
news_ingestion = define_asset_job(
    "news_ingestion", selection=AssetSelection.assets("raw_news")
)


def _scheduled_ingestion(dataset: str, scheduled_for):
    if os.getenv("MARKETFORGE_ENABLE_SCHEDULES") != "1":
        return SkipReason("Local schedules are disabled; set MARKETFORGE_ENABLE_SCHEDULES=1")
    location = os.getenv(f"MARKETFORGE_{dataset.upper()}_INPUT", "")
    if not location:
        return SkipReason(f"MARKETFORGE_{dataset.upper()}_INPUT is not configured")
    source = os.getenv("MARKETFORGE_SOURCE", "configured-provider")
    initial_start = os.getenv("MARKETFORGE_INITIAL_START")
    overlap = int(os.getenv(f"MARKETFORGE_{dataset.upper()}_OVERLAP_DAYS", "0"))
    config = {
        "mode": "incremental", "source": source, "overlap_days": overlap,
        f"{dataset}_input": location,
    }
    if initial_start:
        config["initial_start"] = initial_start
    return RunRequest(
        run_key=f"{dataset}-{scheduled_for.isoformat()}",
        run_config={"resources": {"platform": {"config": config}}},
        tags={"marketforge/dataset": dataset, "marketforge/scheduled": "true"},
    )


@schedule(
    job=prices_ingestion, cron_schedule="15 17 * * 1-5", execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
def prices_after_close_schedule(context):
    return _scheduled_ingestion("prices", context.scheduled_execution_time)


@schedule(
    job=macro_ingestion, cron_schedule="30 6 * * *", execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
def macro_daily_check_schedule(context):
    return _scheduled_ingestion("macro", context.scheduled_execution_time)


@schedule(
    job=fundamentals_ingestion, cron_schedule="0 7 * * *", execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
def fundamentals_daily_check_schedule(context):
    return _scheduled_ingestion("fundamentals", context.scheduled_execution_time)


@schedule(
    job=earnings_ingestion, cron_schedule="30 17 * * 1-5", execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
def earnings_daily_schedule(context):
    return _scheduled_ingestion("earnings", context.scheduled_execution_time)


@schedule(
    job=news_ingestion, cron_schedule="0 */4 * * *", execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
def news_periodic_schedule(context):
    return _scheduled_ingestion("news", context.scheduled_execution_time)


@schedule(
    job=daily_incremental, cron_schedule="15 18 * * 1-5", execution_timezone="America/Chicago",
    default_status=DefaultScheduleStatus.STOPPED,
)
def daily_publish_schedule(context):
    if os.getenv("MARKETFORGE_ENABLE_SCHEDULES") != "1":
        return SkipReason("Local schedules are disabled; set MARKETFORGE_ENABLE_SCHEDULES=1")
    return RunRequest(
        run_key=f"publish-{context.scheduled_execution_time.date().isoformat()}",
        tags={"marketforge/scheduled": "true", "marketforge/purpose": "publish"},
    )


defs = Definitions(
    assets=[
        raw_prices, raw_fundamentals, raw_earnings, raw_macro, raw_news,
        staging_models, intermediate_models, mart_models, quality_gate, api_ready,
    ],
    jobs=[
        daily_incremental, historical_backfill, quality_validation_job, rebuild_marts,
        prices_ingestion, macro_ingestion, fundamentals_ingestion, earnings_ingestion, news_ingestion,
    ],
    schedules=[
        prices_after_close_schedule, macro_daily_check_schedule,
        fundamentals_daily_check_schedule, earnings_daily_schedule,
        news_periodic_schedule, daily_publish_schedule,
    ],
    resources={"platform": PlatformResource()},
)

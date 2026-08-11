"""Lightweight robust distribution monitoring for canonical price data."""

from __future__ import annotations

import json
import math
import os
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import duckdb


METRICS = ("row_count", "null_rate", "median_volume", "median_price", "zero_volume_fraction", "return_dispersion")
DEFAULT_THRESHOLDS = {
    "robust_z_warn": 4.0,
    "robust_z_fail": 8.0,
    "null_rate_warn": 0.001,
    "null_rate_fail": 0.01,
    "zero_volume_warn": 0.05,
    "zero_volume_fail": 0.20,
    "missing_symbol_warn": 0.01,
    "missing_symbol_fail": 0.05,
    "minimum_baseline_days": 20,
    "baseline_days": 60,
}


@dataclass(frozen=True)
class Finding:
    metric: str
    severity: str
    value: float
    reason: str
    baseline_median: float | None = None
    baseline_mad: float | None = None
    robust_z: float | None = None


@dataclass(frozen=True)
class QualityResult:
    dataset: str
    source: str
    event_date: str | None
    status: str
    reason: str
    baseline_days: int
    expected_symbols: int
    observed_symbols: int
    missing_symbols: tuple[str, ...]
    metrics: dict[str, float | None]
    findings: tuple[Finding, ...]


def _daily_profiles(raw_root: Path, source: str) -> list[dict[str, Any]]:
    pattern = str(raw_root / "prices/year=*/month=*/*.parquet")
    if not list((raw_root / "prices").glob("year=*/month=*/*.parquet")):
        return []
    with duckdb.connect() as connection:
        rows = connection.execute(
            """
            WITH base AS (
                SELECT date::DATE event_date, symbol, close::DOUBLE close_value,
                       volume::DOUBLE volume_value,
                       open, high, low,
                       lag(close::DOUBLE) OVER (PARTITION BY symbol, source ORDER BY date) prior_close
                FROM read_parquet(?, hive_partitioning=false) WHERE lower(source) = lower(?)
            )
            SELECT event_date, count(*)::DOUBLE row_count,
                   avg(CASE WHEN symbol IS NULL OR close_value IS NULL OR volume_value IS NULL
                            OR open IS NULL OR high IS NULL OR low IS NULL THEN 1.0 ELSE 0.0 END) null_rate,
                   median(volume_value) median_volume, median(close_value) median_price,
                   avg(CASE WHEN volume_value = 0 THEN 1.0 ELSE 0.0 END) zero_volume_fraction,
                   stddev_pop(CASE WHEN prior_close > 0 THEN close_value / prior_close - 1 END) return_dispersion,
                   count(DISTINCT symbol)::DOUBLE observed_symbols
            FROM base GROUP BY event_date ORDER BY event_date
            """,
            [pattern, source],
        ).fetchall()
    names = ("event_date", *METRICS, "observed_symbols")
    return [dict(zip(names, row)) for row in rows]


def _robust_stats(values: Iterable[float]) -> tuple[float, float]:
    values = list(values)
    median = float(statistics.median(values))
    mad = float(statistics.median(abs(value - median) for value in values))
    return median, mad


def _observed_symbols(raw_root: Path, source: str, event_date: str) -> set[str]:
    pattern = str(raw_root / "prices/year=*/month=*/*.parquet")
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT DISTINCT upper(symbol) FROM read_parquet(?, hive_partitioning=false) "
            "WHERE lower(source)=lower(?) AND date::DATE=?::DATE",
            [pattern, source, event_date],
        ).fetchall()
    return {row[0] for row in rows}


def evaluate_price_quality(
    raw_root: Path = Path("data/raw"), *, source: str = "tiingo",
    expected_symbols: Iterable[str] = (), thresholds: dict[str, float] | None = None,
) -> QualityResult:
    config = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    profiles = _daily_profiles(raw_root, source)
    expected = {symbol.strip().upper() for symbol in expected_symbols if symbol.strip()}
    if not profiles:
        return QualityResult("prices", source, None, "UNKNOWN", "no price observations", 0, len(expected), 0, tuple(sorted(expected)), {}, ())
    current = profiles[-1]
    history = profiles[max(0, len(profiles) - 1 - int(config["baseline_days"])):-1]
    observed = _observed_symbols(raw_root, source, str(current["event_date"]))
    missing = tuple(sorted(expected - observed))
    findings: list[Finding] = []

    def absolute(metric: str, warn: float, fail: float) -> None:
        value = float(current[metric] or 0)
        severity = "FAILED" if value >= fail else "DEGRADED" if value >= warn else None
        if severity:
            findings.append(Finding(metric, severity, value, f"{metric} {value:.4f} exceeds {severity.lower()} threshold"))

    absolute("null_rate", config["null_rate_warn"], config["null_rate_fail"])
    absolute("zero_volume_fraction", config["zero_volume_warn"], config["zero_volume_fail"])
    missing_fraction = len(missing) / len(expected) if expected else 0.0
    severity = "FAILED" if missing_fraction >= config["missing_symbol_fail"] else "DEGRADED" if missing_fraction >= config["missing_symbol_warn"] else None
    if severity:
        findings.append(Finding("missing_symbol_fraction", severity, missing_fraction, f"{len(missing)} of {len(expected)} expected symbols are missing"))

    enough_history = len(history) >= int(config["minimum_baseline_days"])
    if enough_history:
        for metric in ("row_count", "median_volume", "median_price", "return_dispersion"):
            values = [float(row[metric]) for row in history if row[metric] is not None and math.isfinite(float(row[metric]))]
            value = current[metric]
            if len(values) < int(config["minimum_baseline_days"]) or value is None:
                continue
            median, mad = _robust_stats(values)
            scale = max(1.4826 * mad, max(abs(median) * 0.01, 1e-9))
            score = abs(float(value) - median) / scale
            level = "FAILED" if score >= config["robust_z_fail"] else "DEGRADED" if score >= config["robust_z_warn"] else None
            if level:
                findings.append(Finding(metric, level, float(value), f"robust z-score {score:.2f} exceeds {level.lower()} threshold", median, mad, score))

    status = "FAILED" if any(item.severity == "FAILED" for item in findings) else "DEGRADED" if findings else "HEALTHY"
    reason = "; ".join(item.reason for item in findings) if findings else "all configured distribution checks passed"
    if not enough_history and status == "HEALTHY":
        status, reason = "UNKNOWN", f"only {len(history)} baseline days; {int(config['minimum_baseline_days'])} required"
    metrics = {metric: (float(current[metric]) if current[metric] is not None else None) for metric in METRICS}
    metrics["missing_symbol_fraction"] = missing_fraction
    return QualityResult("prices", source, str(current["event_date"]), status, reason, len(history), len(expected), len(observed), missing, metrics, tuple(findings))


def write_quality_audit(result: QualityResult, root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"{result.dataset}-{result.source}.json"
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(asdict(result), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)
    return target

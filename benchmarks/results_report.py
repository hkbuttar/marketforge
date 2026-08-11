#!/usr/bin/env python3
"""Render the evidence-backed final engineering comparison report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


RELIABILITY = (
    ("Provider HTTP 503", "Retry bounded request", "No row written; retry accepted once", "No", "No", "No—retry required"),
    ("Disk full before write", "Leave no canonical fragment", "No canonical file; same run retried", "No", "No", "No—retry required"),
    ("Crash after promotion", "Replay without duplicate; advance checkpoint", "Replay deduplicated; checkpoint advanced", "No", "No", "No—restart required"),
    ("dbt failure", "Raw remains queryable; retry transform", "Raw retained; retry succeeded", "No", "No", "No—retry required"),
)

QUALITY = (
    ("Duplicate row", "Yes", "Yes", "No"),
    ("Missing required column", "Yes", "Batch rejected", "No"),
    ("Negative volume", "Yes", "Yes", "No"),
    ("High below low", "Yes", "Yes", "No"),
    ("Wrong date type", "Yes", "Yes", "No"),
    ("Unknown security", "Yes", "Quality gate", "No"),
    ("Added schema column", "Yes", "Yes", "No"),
    ("Nonnumeric numeric field", "Yes", "Yes", "No"),
    ("Empty API response", "Yes", "No payload", "No; checkpoint unchanged"),
)


def _table(headers: tuple[str, ...], rows) -> str:
    return "\n".join((
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" if index == 0 else "---:" for index in range(len(headers))) + " |",
        *("| " + " | ".join(str(value) for value in row) + " |" for row in rows),
    ))


def build_report(suite: dict[str, Any], partitions: dict[str, Any], serving: dict[str, Any]) -> str:
    formats = suite["storage"]["formats"]
    storage_rows = [
        (("CSV" if name == "csv" else name.replace("parquet_", "Parquet ").title()), metrics["bytes"],
         metrics["write_ms"], metrics["aggregation_median_ms"])
        for name, metrics in formats.items()
    ]
    layout_rows = []
    for name, metrics in partitions["layouts"].items():
        layout_rows.append((name.replace("_", "/"), metrics["files"], metrics["bytes"],
                            metrics["queries"]["one_month_one_security"]["median_ms"],
                            metrics["queries"]["full_history_aggregation"]["median_ms"]))
    process_rows = [
        (label, values["wall_clock_seconds"], values["peak_ram_bytes"],
         values["bytes_read"], values["bytes_written"])
        for label, values in (("Full refresh", suite["full_refresh"]), ("Daily incremental", suite["incremental"]))
    ]
    compact = suite["compaction"]
    serving_rows = [
        (endpoint, values["cold_median_ms"], values["cold_p95_ms"],
         values["warm_median_ms"], values["warm_p95_ms"])
        for endpoint, values in serving["endpoints"].items()
    ]
    return f"""# Results and honest comparison

Measurements were produced on the local CPU-only machine and the retained
{suite['historical_rows']:,}-row price lake. They are reproducible observations,
not concurrency, cloud-scale, or cross-machine claims.

Numeric sources: `benchmarks/results/latest.json`, `partitions.json`, and
`serving.json`. Reliability and quality observations are enforced by the failure
and synthetic-defect test suites.

## Storage

{_table(('Format', 'Bytes', 'Write ms', 'Aggregation median ms'), storage_rows)}

ZSTD uses **{formats['parquet_zstd']['space_saved_percent']:.2f}% less space than CSV**,
but writes more slowly than CSV and aggregates slightly more slowly than Snappy.
That CPU cost is accepted because disk is the explicit constraint.

## Partition strategy

{_table(('Layout', 'Files', 'Bytes', 'Month/symbol ms', 'Full aggregate ms'), layout_rows)}

The single file wins pure query latency at this scale, but cannot support immutable
incremental appends. Year/month is the operational compromise. Year/month/symbol
is rejected: its small-file discovery and storage overhead are dramatically worse.

## Processing

{_table(('Path', 'Runtime s', 'Peak RAM bytes', 'Bytes read', 'Bytes written'), process_rows)}

The incremental path is **{suite['incremental_comparison']['runtime_speedup']:.2f}× faster**
and writes **{suite['incremental_comparison']['write_reduction_percent']:.2f}% fewer bytes**,
with canonical equivalence verified before reporting.

## Compaction

{_table(('State', 'Files', 'Bytes', 'Count-query median ms'), (
    ('Before', compact['file_count_before'], compact['bytes_before'], compact['latency_before_ms']),
    ('After', compact['file_count_after'], compact['bytes_after'], compact['latency_after_ms']),
))}

## Reliability

{_table(('Failure', 'Expected', 'Observed', 'Data loss?', 'Duplicates?', 'Automatic recovery?'), RELIABILITY)}

## Quality

{_table(('Injected defect', 'Detected?', 'Quarantined/action', 'Downstream contamination?'), QUALITY)}

## Serving

{_table(('Endpoint', 'Cold median ms', 'Cold p95 ms', 'Warm median ms', 'Warm p95 ms'), serving_rows)}

Serving results are sequential in-process FastAPI measurements over marts derived
from retained Tiingo prices. They do not establish throughput under concurrency.

## Where sophistication did not help

- A single Parquet file is faster, but loses append-only operational behavior.
- Year/month/symbol partitioning performs far worse than monthly partitions here.
- The in-process cache lowers warm endpoint latency; no result demonstrates a need for Redis.
- SQLite satisfies the tested transactional metadata requirements; Postgres was not benchmarked.
- Kafka remains optional: HTTP polling is simpler when gap-free delivery is not required.
- ZSTD's extra write CPU is measurable; it is retained only because its disk savings matter.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, default=Path("benchmarks/results/latest.json"))
    parser.add_argument("--partitions", type=Path, default=Path("benchmarks/results/partitions.json"))
    parser.add_argument("--serving", type=Path, default=Path("benchmarks/results/serving.json"))
    parser.add_argument("--output", type=Path, default=Path("docs/results.md"))
    args = parser.parse_args()
    report = build_report(
        json.loads(args.suite.read_text()), json.loads(args.partitions.read_text()),
        json.loads(args.serving.read_text()),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

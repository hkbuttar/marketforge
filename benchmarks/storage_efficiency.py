#!/usr/bin/env python3
"""Compare CSV and Parquet encodings using a representative local dataset."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb


FORMATS = {
    "csv": ("csv", None),
    "parquet_uncompressed": ("parquet", "UNCOMPRESSED"),
    "parquet_snappy": ("parquet", "SNAPPY"),
    "parquet_zstd": ("parquet", "ZSTD"),
}


def _timed(action) -> float:
    started = time.perf_counter()
    action()
    return (time.perf_counter() - started) * 1000


def benchmark_storage(raw_root: Path, output_root: Path, iterations: int = 5) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    files = sorted((raw_root / "prices").glob("year=*/month=*/*.parquet"))
    if not files:
        raise ValueError("no price Parquet files found")
    pattern = str(raw_root / "prices/year=*/month=*/*.parquet")
    output_root.mkdir(parents=True, exist_ok=True)
    results = {}
    with duckdb.connect() as connection:
        escaped_pattern = pattern.replace("'", "''")
        connection.execute(
            "CREATE TEMP VIEW representative_prices AS SELECT * FROM "
            f"read_parquet('{escaped_pattern}', hive_partitioning=false, union_by_name=true)"
        )
        rows = connection.execute("SELECT count(*) FROM representative_prices").fetchone()[0]
        columns = len(connection.execute("DESCRIBE representative_prices").fetchall())
        for name, (kind, compression) in FORMATS.items():
            suffix = "csv" if kind == "csv" else "parquet"
            target = output_root / f"prices-{name}.{suffix}"
            escaped = str(target).replace("'", "''")
            if kind == "csv":
                copy = f"COPY representative_prices TO '{escaped}' (FORMAT CSV, HEADER true)"
                relation = f"read_csv_auto('{escaped}', header=true)"
            else:
                copy = (
                    f"COPY representative_prices TO '{escaped}' "
                    f"(FORMAT PARQUET, COMPRESSION {compression})"
                )
                relation = f"read_parquet('{escaped}', hive_partitioning=false)"
            write_ms = _timed(lambda command=copy: connection.execute(command))
            read_samples = []
            aggregation_samples = []
            for _ in range(iterations):
                read_samples.append(_timed(
                    lambda source=relation: connection.execute(f"SELECT * FROM {source}").fetchall()
                ))
                aggregation_samples.append(_timed(
                    lambda source=relation: connection.execute(
                        f'SELECT symbol, avg("close"), sum(volume) FROM {source} GROUP BY symbol'
                    ).fetchall()
                ))
            results[name] = {
                "bytes": target.stat().st_size,
                "write_ms": round(write_ms, 3),
                "read_median_ms": round(statistics.median(read_samples), 3),
                "aggregation_median_ms": round(statistics.median(aggregation_samples), 3),
            }
    csv_bytes = results["csv"]["bytes"]
    for metrics in results.values():
        metrics["size_ratio_to_csv"] = round(metrics["bytes"] / csv_bytes, 4)
        metrics["space_saved_percent"] = round((1 - metrics["bytes"] / csv_bytes) * 100, 2)
    return {
        "dataset": "prices", "rows": rows, "columns": columns,
        "source_files": len(files), "iterations": iterations, "formats": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="marketforge-storage-") as directory:
        result = benchmark_storage(args.raw_root, Path(directory), args.iterations)
    output = json.dumps(result, indent=2)
    if args.results:
        args.results.parent.mkdir(parents=True, exist_ok=True)
        args.results.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Run a command and append a compact, machine-readable resource benchmark."""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_RESULTS = Path("benchmarks/results.jsonl")


def path_size(path: Path) -> int:
    """Return allocated file content bytes without following directory symlinks."""
    try:
        if path.is_symlink():
            return 0
        if path.is_file():
            return path.stat().st_size
        if path.is_dir():
            return sum(path_size(child) for child in path.iterdir())
    except (FileNotFoundError, PermissionError):
        return 0
    return 0


def paths_size(paths: list[Path]) -> int:
    return sum(path_size(path) for path in paths)


def peak_rss_bytes(usage: resource.struct_rusage) -> int:
    # macOS reports bytes; Linux and the other supported Unix hosts report KiB.
    return int(usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", required=True, help="Stable name for the measured job")
    parser.add_argument("--input", action="append", default=[], type=Path)
    parser.add_argument("--output", action="append", default=[], type=Path)
    parser.add_argument("--rows", required=True, type=int, help="Logical rows processed")
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--max-seconds", type=float, default=300.0)
    parser.add_argument("--max-ram-mb", type=float, default=4096.0)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.rows < 0:
        parser.error("--rows must be non-negative")
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after --")
    return args


def main() -> int:
    args = parse_args()
    input_bytes = paths_size(args.input)
    output_before = paths_size(args.output)
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started_at = datetime.now(timezone.utc)
    start = time.monotonic()
    completed = subprocess.run(args.command, check=False)
    elapsed = time.monotonic() - start
    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    output_total = paths_size(args.output)

    user_seconds = max(0.0, usage_after.ru_utime - usage_before.ru_utime)
    system_seconds = max(0.0, usage_after.ru_stime - usage_before.ru_stime)
    peak_bytes = peak_rss_bytes(usage_after)
    cpu_percent = ((user_seconds + system_seconds) / elapsed * 100) if elapsed else 0.0
    breaches = []
    if elapsed > args.max_seconds:
        breaches.append("runtime")
    if peak_bytes > args.max_ram_mb * 1_000_000:
        breaches.append("peak_ram")

    record = {
        "schema_version": 1,
        "job": args.job,
        "started_at": started_at.isoformat(),
        "command": args.command,
        "exit_code": completed.returncode,
        "cpu": {
            "logical_cores": os.cpu_count(),
            "architecture": platform.machine(),
            "user_seconds": round(user_seconds, 6),
            "system_seconds": round(system_seconds, 6),
            "utilization_percent": round(cpu_percent, 2),
        },
        "peak_ram_bytes": peak_bytes,
        "wall_clock_seconds": round(elapsed, 6),
        "input_bytes": input_bytes,
        "output_bytes": max(0, output_total - output_before),
        "output_total_bytes": output_total,
        "rows_processed": args.rows,
        "limits": {
            "max_seconds": args.max_seconds,
            "max_ram_mb": args.max_ram_mb,
            "breaches": breaches,
        },
    }
    args.results.parent.mkdir(parents=True, exist_ok=True)
    with args.results.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")
    print(json.dumps(record, indent=2))
    return completed.returncode or (2 if breaches else 0)


if __name__ == "__main__":
    raise SystemExit(main())

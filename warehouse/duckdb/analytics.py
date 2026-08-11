"""Install DuckDB views over retained Parquet without copying lake data."""

from __future__ import annotations

from pathlib import Path

import duckdb


DATASETS = ("prices", "fundamentals", "earnings", "macro", "news")


def install_raw_views(connection: duckdb.DuckDBPyConnection, raw_root: Path = Path("data/raw")) -> None:
    for dataset in DATASETS:
        files = list((raw_root / dataset).glob("year=*/month=*/*.parquet"))
        if not files:
            continue
        glob = str(raw_root / dataset / "year=*" / "month=*" / "*.parquet").replace("'", "''")
        connection.execute(
            f"""CREATE OR REPLACE VIEW raw_{dataset} AS
                SELECT * FROM read_parquet(
                    '{glob}', hive_partitioning=true, union_by_name=false
                )"""
        )

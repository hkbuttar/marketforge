"""Security-universe resolution checks kept separate from source contracts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


def load_symbols(path: Path = Path("config/price_universe.txt")) -> frozenset[str]:
    symbols = {
        line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    if not symbols:
        raise ValueError("security universe is empty")
    return frozenset(symbols)


def unknown_symbols(records: Iterable[Mapping[str, Any]], known: Iterable[str]) -> tuple[str, ...]:
    universe = {symbol.strip().upper() for symbol in known}
    observed = {
        str(row.get("symbol", row.get("ticker", ""))).strip().upper()
        for row in records
    }
    return tuple(sorted(symbol for symbol in observed if symbol and symbol not in universe))

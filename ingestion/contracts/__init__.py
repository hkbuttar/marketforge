"""Executable source contracts."""

from .earnings import EARNINGS_CONTRACT
from .fundamentals import FUNDAMENTALS_CONTRACT
from .macro import MACRO_CONTRACT
from .news import NEWS_CONTRACT
from .prices import PRICES_CONTRACT

CONTRACTS = {
    contract.name: contract
    for contract in (
        PRICES_CONTRACT,
        FUNDAMENTALS_CONTRACT,
        EARNINGS_CONTRACT,
        MACRO_CONTRACT,
        NEWS_CONTRACT,
    )
}

__all__ = [
    "CONTRACTS",
    "PRICES_CONTRACT",
    "FUNDAMENTALS_CONTRACT",
    "EARNINGS_CONTRACT",
    "MACRO_CONTRACT",
    "NEWS_CONTRACT",
]

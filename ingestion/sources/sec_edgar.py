"""SEC EDGAR public company-facts adapter."""

from __future__ import annotations

import os

from .http_json import SourceHTTPError, get_json


BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
METRICS = (
    "RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues",
    "NetIncomeLoss", "Assets", "Liabilities", "StockholdersEquity",
)


def fetch_fundamentals(symbol: str, cik: str, *, user_agent: str | None = None) -> list[dict]:
    agent = user_agent or os.getenv("SEC_USER_AGENT")
    if not agent or " " not in agent:
        raise SourceHTTPError("SEC_USER_AGENT must contain an application name and contact email")
    payload = get_json(
        f"{BASE_URL}/CIK{int(cik):010d}.json",
        headers={"User-Agent": agent, "Accept-Encoding": "gzip, deflate"},
    )
    facts = payload.get("facts", {}).get("us-gaap", {})
    rows, identities = [], set()
    for metric in METRICS:
        fact = facts.get(metric)
        if not fact:
            continue
        for unit, observations in fact.get("units", {}).items():
            for item in observations:
                if item.get("form") not in {"10-K", "10-Q"} or not item.get("filed"):
                    continue
                identity = f"{item['accn']}:{metric}:{item['end']}:{unit}"
                if identity in identities:
                    continue
                identities.add(identity)
                rows.append({
                    "symbol": symbol, "metric_name": metric,
                    "period_start": item.get("start"), "period_end": item["end"],
                    "period_type": item.get("fp") or item["form"],
                    "filed_at": f"{item['filed']}T00:00:00Z", "value": item["val"],
                    "unit": unit, "currency": unit if unit in {"USD", "EUR", "GBP"} else None,
                    "source_record_id": identity,
                })
    return rows

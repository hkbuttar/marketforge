from .base import Contract, Field, finite_float, integer, iso_date, text, upper_text, utc_datetime


def price_ranges(row):
    for name in ("open", "high", "low", "close"):
        if row[name] <= 0:
            return f"{name}: must be greater than zero"
    if row["volume"] < 0:
        return "volume: must be non-negative"
    if row["high"] < max(row["low"], row["open"], row["close"]):
        return "high: must be at least low, open, and close"
    if row["low"] > min(row["open"], row["close"]):
        return "low: must be at most open and close"
    return None


PRICES_CONTRACT = Contract(
    name="prices",
    fields={
        "symbol": Field(upper_text, description="Source symbol"),
        "date": Field(iso_date, description="Trading event date"),
        "open": Field(finite_float),
        "high": Field(finite_float),
        "low": Field(finite_float),
        "close": Field(finite_float),
        "volume": Field(integer),
        "source": Field(text),
        "source_record_id": Field(text),
        "ingested_at": Field(utc_datetime),
    },
    unique_by=("source", "source_record_id"),
    rules=(price_ranges,),
    aliases={"trade_date": "date", "ticker": "symbol"},
    source_metadata={"frequency": "daily", "event_time_field": "date"},
)

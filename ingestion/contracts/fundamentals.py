from .base import Contract, Field, finite_float, iso_date, text, upper_text, utc_datetime


def period_order(row):
    if row["period_start"] is not None and row["period_end"] < row["period_start"]:
        return "period_end: must not precede period_start"
    if row["filed_at"] is not None and row["filed_at"] > row["ingested_at"]:
        return "filed_at: must not be later than ingested_at"
    return None


FUNDAMENTALS_CONTRACT = Contract(
    name="fundamentals",
    fields={
        "symbol": Field(upper_text),
        "metric_name": Field(text),
        "period_start": Field(iso_date, nullable=True),
        "period_end": Field(iso_date),
        "period_type": Field(upper_text),
        "filed_at": Field(utc_datetime, nullable=True),
        "value": Field(finite_float),
        "unit": Field(upper_text),
        "currency": Field(upper_text, nullable=True),
        "source": Field(text),
        "source_record_id": Field(text),
        "ingested_at": Field(utc_datetime),
    },
    unique_by=("source", "source_record_id"),
    idempotency_by=("source", "source_record_id"),
    rules=(period_order,),
    aliases={"ticker": "symbol", "fiscal_period_end": "period_end"},
    source_metadata={"event_time_field": "period_end", "knowledge_time_field": "filed_at"},
)

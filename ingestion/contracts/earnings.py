from .base import Contract, Field, finite_float, iso_date, text, upper_text, utc_datetime


EARNINGS_CONTRACT = Contract(
    name="earnings",
    fields={
        "symbol": Field(upper_text),
        "event_timestamp": Field(utc_datetime),
        "fiscal_period_end": Field(iso_date, nullable=True),
        "event_status": Field(upper_text),
        "eps_estimate": Field(finite_float, nullable=True),
        "eps_actual": Field(finite_float, nullable=True),
        "source": Field(text),
        "source_record_id": Field(text),
        "ingested_at": Field(utc_datetime),
    },
    unique_by=("source", "source_record_id"),
    aliases={"ticker": "symbol", "event_time": "event_timestamp"},
    source_metadata={"event_time_field": "event_timestamp", "supports_scheduled_events": True},
)

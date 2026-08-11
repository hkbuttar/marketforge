from .base import Contract, Field, finite_float, iso_date, text, upper_text, utc_datetime


def release_order(row):
    if row["released_at"] is not None and row["released_at"] > row["ingested_at"]:
        return "released_at: must not be later than ingested_at"
    return None


MACRO_CONTRACT = Contract(
    name="macro",
    fields={
        "series_id": Field(upper_text),
        "observation_date": Field(iso_date),
        "released_at": Field(utc_datetime, nullable=True),
        "value": Field(finite_float),
        "unit": Field(upper_text),
        "frequency": Field(upper_text),
        "source": Field(text),
        "source_record_id": Field(text),
        "ingested_at": Field(utc_datetime),
    },
    unique_by=("source", "source_record_id"),
    rules=(release_order,),
    aliases={"date": "observation_date", "release_timestamp": "released_at"},
    source_metadata={"event_time_field": "observation_date", "knowledge_time_field": "released_at"},
)

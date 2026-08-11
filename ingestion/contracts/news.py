from urllib.parse import urlparse

from .base import Contract, Field, text, utc_datetime


def url(value):
    value = text(value)
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("must be an absolute HTTP(S) URL")
    return value


NEWS_CONTRACT = Contract(
    name="news",
    fields={
        "event_timestamp": Field(utc_datetime),
        "headline": Field(text),
        "url": Field(url, nullable=True),
        "publisher": Field(text, nullable=True),
        "source": Field(text),
        "source_record_id": Field(text),
        "ingested_at": Field(utc_datetime),
    },
    unique_by=("source", "source_record_id"),
    idempotency_by=("source", "source_record_id"),
    aliases={"published_at": "event_timestamp", "title": "headline"},
    source_metadata={"event_time_field": "event_timestamp", "content_retained": False},
)

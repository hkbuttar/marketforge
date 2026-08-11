# Optional StreamAlpha streaming bridge

## Hosted backend polling

The deployed StreamAlpha backend can be used without broker access. Its public
`/anomalies` response is converted to the same event envelope and durable Parquet
layout used by the Kafka bridge:

```bash
export STREAMALPHA_BACKEND_URL=https://streamalpha-backend.onrender.com
python -m scripts.poll_streamalpha --limit 500
```

Optional `--ticker` and `--anomaly-type` filters are passed to the backend. A
stable event ID is derived from the complete anomaly record, so overlapping polls
are safe: records already stored locally are counted as duplicates. The original
`details` object is retained in `details_json`, and `detected_at` is retained as a
timestamp.

The current endpoint has a bounded `limit`, but no cursor or `since` parameter.
Polling therefore provides replay-safe snapshots, not a gap-free event stream. If
more events arrive between polls than the selected limit, older unseen events can
be missed. Kafka remains the appropriate integration when strict continuous
delivery is required.

## Kafka consumer

Kafka is not part of MarketForge's core runtime. The narrow bridge consumes a
bounded micro-batch from a StreamAlpha topic and writes immutable ZSTD Parquet to:

```text
data/raw/stream_events/date=YYYY-MM-DD/hour=HH/part-<run-id>.parquet
```

Expected JSON envelope:

```json
{
  "event_id": "stable-streamalpha-id",
  "symbol": "AAPL",
  "event_timestamp": "2026-08-10T15:30:00Z",
  "event_type": "price_anomaly",
  "price": 101.5,
  "anomaly_score": 3.2
}
```

`event_id`, symbol, timestamp, and type are required. Price and anomaly score are
nullable. MarketForge adds topic, partition, offset, and ingestion time. It
deduplicates both the logical event ID and Kafka coordinates.

Install and run only when a broker is available:

```bash
pip install -e '.[streaming]'
export STREAMALPHA_KAFKA_BROKERS=localhost:9092
export STREAMALPHA_KAFKA_TOPIC=streamalpha.events
python -m scripts.consume_streamalpha --max-records 500 --max-wait-seconds 1
```

Auto-commit is disabled. Offsets are synchronously committed only after all final
Parquet files are durable. A crash after promotion but before commit causes Kafka
redelivery; retained event IDs/coordinates turn that replay into a no-op before the
offset is acknowledged. Invalid messages are written to an immutable quarantine
artifact before their offsets are committed, preventing a poison message from
blocking the local consumer forever.

The optional `mart_intraday_anomalies` joins every event to information knowable
before the anomaly: the prior completed session's return, 20-day volatility and
relative volume; the latest earlier earnings event; and five-session security and
market returns with their excess-return factor. The strict `< event date` join
prevents an intraday anomaly from seeing that day's closing bar. The legacy
`mart_stream_anomalies` relation remains available as the underlying table.
Enable the integration only after stream files exist:

```bash
dbt build --project-dir dbt --profiles-dir dbt \
  --vars '{enable_streamalpha: true}' --select +mart_intraday_anomalies
```

No broker or Kafka package is required for normal batch ingestion, analytics,
tests, or serving.

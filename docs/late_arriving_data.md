# Late-arriving data

MarketForge detects a late arrival when a newly accepted record's event date is
at or before the dataset/source watermark that existed when an incremental run
started. Configure a provider overlap window large enough to request those older
records:

```bash
python -m ingestion.cli run \
  --dataset prices --source provider-name --input extracts/latest.jsonl \
  --through-date 2026-07-18 --overlap-days 3
```

For the controlled test case, an event dated `2026-07-15` arrives during the
`2026-07-18` run after the watermark has reached `2026-07-17`. It is appended to
the existing July raw partition as a new immutable Parquet fragment. The manifest
records:

- `earliest_late_event_date`: event time (date for daily datasets)
- `arrival_time`: when MarketForge received the batch
- `started_at` and `completed_at`: processing-time boundaries
- `prior_event_watermark`: the watermark used to classify the event
- `late_arriving_rows`: newly retained late records, excluding replay duplicates

The checkpoint remains monotonic and advances to the newest accepted event. In an
orchestrated run, the changed raw asset causes its staging asset and declared
downstream intermediate/mart dependencies to rebuild; unrelated source branches
are not selected. Dagster materialization metadata exposes the late-row count and
earliest late event for operators.

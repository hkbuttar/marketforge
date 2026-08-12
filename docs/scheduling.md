# Optional local schedules

Scheduling is opt-in. Every schedule is registered as `STOPPED`, and schedule
evaluation returns a skip unless `MARKETFORGE_ENABLE_SCHEDULES=1`. MarketForge does
not assume a laptop is permanently online; missed runs are recovered through
checkpoint windows and idempotent overlap processing.

All times use `America/Chicago` explicitly:

| Schedule | Cadence | Purpose |
| --- | --- | --- |
| `macro_daily_check_schedule` | 06:30 daily | Check configured macro extract; unchanged identities write nothing. |
| `fundamentals_daily_check_schedule` | 07:00 daily | Check for new filings/fundamentals. |
| `prices_after_close_schedule` | 17:15 weekdays | Ingest after the U.S. market close with optional overlap. |
| `earnings_daily_schedule` | 17:30 weekdays | Ingest scheduled/reported earnings events. |
| `news_periodic_schedule` | Every four hours | Optional bounded news metadata batch. |
| `daily_publish_schedule` | 18:15 weekdays | Observe successful raw assets, rebuild dbt layers, test freshness, then mark API-ready. |

Each source schedule requires `MARKETFORGE_<DATASET>_INPUT`; absent inputs produce a
visible Dagster skip rather than a failed or empty ingestion. Configure the common
provider with `MARKETFORGE_SOURCE`, the first checkpoint date with
`MARKETFORGE_INITIAL_START`, and revision windows with
`MARKETFORGE_<DATASET>_OVERLAP_DAYS`.

To enable local scheduling:

```bash
export DAGSTER_HOME="$PWD/config/dagster"
export MARKETFORGE_ENABLE_SCHEDULES=1
export MARKETFORGE_SOURCE=provider-name
export MARKETFORGE_PRICES_INPUT="$PWD/extracts/prices.jsonl"
.venv/bin/dagster dev -m orchestration.definitions
```

The checked-in Dagster instance configuration allows only one concurrent run and
one concurrent op, protecting the single-writer DuckDB layer on a laptop. Schedule
run keys are date/dataset-specific, preventing duplicate launches for the same
scheduled period.

Manual execution remains first-class and requires no daemon:

```bash
make daily
make quality
# or directly:
.venv/bin/dagster job execute -m orchestration.definitions -j daily_incremental
```

`make daily` runs in observe mode by default: ingestion can be performed with the
CLI or source jobs first, then the full asset graph verifies those artifacts. A
sleeping laptop simply resumes from its retained checkpoints on the next run.

## Live Tiingo schedule

The 100-symbol universe requires two hourly-safe shards on Tiingo's 50-request-per-hour Starter limit. Run shard 0 after market data is available, then shard 1 in the next clock hour:

```bash
set -a; source .env; set +a
python -m scripts.update_tiingo --shard 0 --through YYYY-MM-DD
# next hourly quota window
python -m scripts.update_tiingo --shard 1 --through YYYY-MM-DD
python -m scripts.check_quality
```

Each shard uses a seven-day overlap by default, making weekends, holidays, a sleeping laptop, and recent provider corrections replay-safe. A shard contains at most 50 symbols and therefore never intentionally exceeds the Starter hourly request allowance. Enable these commands in a local scheduler only after confirming `.env` is mode `0600`; never embed the token in a plist, crontab, log, or command argument.

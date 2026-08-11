"""Poll StreamAlpha's anomaly API into the durable stream-event lake."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict

from ingestion.sources.streamalpha import DEFAULT_BASE_URL, PolledAnomalyConsumer, fetch_anomalies
from ingestion.streaming.kafka import consume_microbatch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("STREAMALPHA_BACKEND_URL", DEFAULT_BASE_URL))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--ticker")
    parser.add_argument("--anomaly-type")
    args = parser.parse_args()
    messages = fetch_anomalies(
        base_url=args.base_url, limit=args.limit, ticker=args.ticker,
        anomaly_type=args.anomaly_type,
    )
    consumer = PolledAnomalyConsumer(messages)
    result = consume_microbatch(
        consumer, max_records=max(1, len(messages)), max_wait_seconds=1.0
    )
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

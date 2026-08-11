import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb

from ingestion.streaming.kafka import KafkaMessage, consume_microbatch
from ingestion.sources.streamalpha import (
    PolledAnomalyConsumer,
    StreamAlphaBackendError,
    fetch_anomalies,
)


def message(offset, event_id=None, score=3.0):
    payload = {
        "event_id": event_id or f"event-{offset}", "symbol": "aapl",
        "event_timestamp": "2026-08-10T15:30:00Z", "event_type": "price_anomaly",
        "price": 101.5, "anomaly_score": score,
    }
    return KafkaMessage("streamalpha.events", 0, offset, json.dumps(payload).encode())


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.committed = []

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, messages):
        self.committed.extend(messages)


class FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return self.payload


class StreamAlphaBridgeTests(unittest.TestCase):
    @patch("ingestion.sources.streamalpha.urlopen")
    def test_http_anomalies_are_stable_complete_and_replay_safe(self, urlopen):
        anomaly = {
            "ticker": "csco",
            "window_start": "2026-08-05T16:15:13.709339Z",
            "anomaly_type": "regime_change",
            "details": {"realized_volatility": 0.0, "changepoint_probability": 0.898},
            "detected_at": "2026-08-11T18:31:44.473749Z",
        }
        urlopen.side_effect = [FakeResponse([anomaly]), FakeResponse([anomaly])]
        first_messages = fetch_anomalies(base_url="https://example.test", limit=10)
        replay_messages = fetch_anomalies(base_url="https://example.test", limit=10)
        self.assertEqual(first_messages, replay_messages)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {
                "raw_root": root / "raw",
                "quarantine_root": root / "quarantine",
                "metadata_root": root / "metadata",
            }
            first = consume_microbatch(PolledAnomalyConsumer(first_messages), run_id="http", **options)
            replay = consume_microbatch(PolledAnomalyConsumer(replay_messages), run_id="replay", **options)
            self.assertEqual(first.accepted_events, 1)
            self.assertEqual(replay.duplicate_events, 1)
            parquet = next((root / "raw/stream_events").glob("**/*.parquet"))
            with duckdb.connect() as connection:
                row = connection.execute(
                    "SELECT symbol, anomaly_score, details_json, detected_at FROM read_parquet(?)",
                    [str(parquet)],
                ).fetchone()
            self.assertEqual(row[0], "CSCO")
            self.assertEqual(row[1], 0.898)
            self.assertEqual(json.loads(row[2]), anomaly["details"])
            self.assertIsNotNone(row[3])

    @patch("ingestion.sources.streamalpha.urlopen")
    def test_http_anomaly_requires_backend_contract(self, urlopen):
        urlopen.return_value = FakeResponse([{"ticker": "AAPL"}])
        with self.assertRaises(StreamAlphaBackendError):
            fetch_anomalies(base_url="https://example.test")

    def test_microbatch_is_durable_before_offsets_commit_and_replay_deduplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            messages = [message(1), message(2)]
            first_consumer = FakeConsumer(messages)
            first = consume_microbatch(
                first_consumer, raw_root=root / "raw", quarantine_root=root / "quarantine",
                metadata_root=root / "metadata", run_id="first",
                now=datetime(2026, 8, 11, tzinfo=timezone.utc),
            )
            self.assertEqual(first.accepted_events, 2)
            self.assertEqual(len(first_consumer.committed), 2)
            files = list((root / "raw/stream_events").glob("**/*.parquet"))
            self.assertEqual(len(files), 1)
            with duckdb.connect() as connection:
                count = connection.execute("SELECT count(*) FROM read_parquet(?)", [str(files[0])]).fetchone()[0]
            self.assertEqual(count, 2)

            replay_consumer = FakeConsumer(messages)
            replay = consume_microbatch(
                replay_consumer, raw_root=root / "raw", quarantine_root=root / "quarantine",
                metadata_root=root / "metadata", run_id="replay",
            )
            self.assertEqual(replay.accepted_events, 0)
            self.assertEqual(replay.duplicate_events, 2)
            self.assertEqual(replay.files_written, 0)
            self.assertEqual(len(replay_consumer.committed), 2)

    def test_crash_after_write_does_not_commit_and_restart_is_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            consumer = FakeConsumer([message(1)])

            def crash(stage, _path):
                if stage == "after_write_before_commit":
                    raise RuntimeError("injected crash")

            with self.assertRaises(RuntimeError):
                consume_microbatch(
                    consumer, raw_root=root / "raw", quarantine_root=root / "quarantine",
                    metadata_root=root / "metadata", run_id="crash", failure_hook=crash,
                )
            self.assertFalse(consumer.committed)
            restart = FakeConsumer([message(1)])
            result = consume_microbatch(
                restart, raw_root=root / "raw", quarantine_root=root / "quarantine",
                metadata_root=root / "metadata", run_id="restart",
            )
            self.assertEqual(result.duplicate_events, 1)
            self.assertEqual(len(restart.committed), 1)

    def test_malformed_event_is_quarantined_then_offset_is_committed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            malformed = KafkaMessage("streamalpha.events", 1, 4, b'{"symbol":"AAPL"}')
            consumer = FakeConsumer([malformed])
            result = consume_microbatch(
                consumer, raw_root=root / "raw", quarantine_root=root / "quarantine",
                metadata_root=root / "metadata", run_id="bad",
            )
            self.assertEqual(result.status, "degraded")
            self.assertEqual(result.quarantined_events, 1)
            self.assertEqual(len(consumer.committed), 1)
            self.assertTrue((root / "quarantine/run=bad.jsonl").exists())


if __name__ == "__main__":
    unittest.main()

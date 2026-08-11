import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingestion.loaders import run_backfill


DBT = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
if not Path(DBT).is_file():
    DBT = None
ROOT = Path(__file__).parents[2]
NOW = datetime(2026, 8, 11, tzinfo=timezone.utc)


@unittest.skipUnless(DBT, "dbt is not installed in this environment")
class DbtStagingTests(unittest.TestCase):
    def test_all_staging_models_build_and_normalize(self):
        records = {
            "prices": {"symbol": "aapl", "date": "2026-08-10", "open": 100, "high": 102,
                       "low": 99, "close": 101, "volume": 10, "source_record_id": "p1"},
            "fundamentals": {"symbol": "aapl", "metric_name": "Revenue", "period_start": "2026-04-01",
                             "period_end": "2026-06-30", "period_type": "quarter", "filed_at": "2026-08-01T00:00:00Z",
                             "value": 1.0, "unit": "usd", "currency": "usd", "source_record_id": "f1"},
            "earnings": {"symbol": "aapl", "event_timestamp": "2026-08-01T00:00:00Z",
                         "fiscal_period_end": "2026-06-30", "event_status": "reported",
                         "eps_estimate": 1.0, "eps_actual": 1.1, "source_record_id": "e1"},
            "macro": {"series_id": "cpi", "observation_date": "2026-07-01",
                      "released_at": "2026-08-01T00:00:00Z", "value": 100, "unit": "index",
                      "frequency": "monthly", "source_record_id": "m1"},
            "news": {"event_timestamp": "2026-08-01T00:00:00Z", "headline": " Example headline ",
                     "url": "https://example.com/story", "publisher": "Example", "source_record_id": "n1"},
        }
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            raw = temp / "raw"
            for dataset, record in records.items():
                run_backfill(
                    dataset, [record], source="Test-Provider", raw_root=raw,
                    quarantine_root=temp / "quarantine", metadata_root=temp / "metadata",
                    run_id=f"dbt-{dataset}", now=NOW,
                )
            database = temp / "transform.duckdb"
            profiles = temp / "profiles"
            profiles.mkdir()
            (profiles / "profiles.yml").write_text(
                "marketforge:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n"
                f"      path: '{database}'\n      schema: main\n      threads: 2\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [DBT, "build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(profiles),
                 "--vars", json.dumps({"raw_root": str(raw)}), "--no-use-colors"],
                cwd=ROOT, text=True, capture_output=True, check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            with duckdb.connect(str(database)) as connection:
                models = connection.execute(
                    "SELECT table_name FROM information_schema.views WHERE table_schema='main_staging'"
                ).fetchall()
                self.assertEqual({row[0] for row in models}, {
                    "stg_prices", "stg_fundamentals", "stg_earnings", "stg_macro", "stg_news"
                })
                price = connection.execute(
                    "SELECT symbol, source, length(price_bar_key) FROM main_staging.stg_prices"
                ).fetchone()
                self.assertEqual(price, ("AAPL", "test-provider", 32))
                self.assertEqual(connection.execute(
                    "SELECT count(*) FROM main_staging.stg_news WHERE headline='Example headline'"
                ).fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()

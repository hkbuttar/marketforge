import json
import shutil
import sqlite3
from contextlib import closing
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from backend.main import create_app
from ingestion.loaders import run_backfill
from ingestion.sources.files import read_records


ROOT = Path(__file__).parents[2]
DBT = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
if not Path(DBT).is_file():
    DBT = None


@unittest.skipUnless(DBT, "dbt is not installed in this environment")
class EndToEndPlatformTests(unittest.TestCase):
    def test_known_record_and_rejection_survive_source_to_api(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            metadata = root / "metadata"
            records = {
                dataset: read_records(str(ROOT / f"tests/fixtures/ci/{dataset}.jsonl"))
                for dataset in ("prices", "fundamentals", "earnings", "macro", "news")
            }
            records["prices"].append({
                "symbol": "REJECT", "date": "2026-08-10", "open": 100,
                "high": 90, "low": 99, "close": 101, "volume": 10,
                "source_record_id": "invalid-price",
            })
            results = {}
            for dataset, rows in records.items():
                results[dataset] = run_backfill(
                    dataset, rows, source="Test-Provider", raw_root=raw,
                    quarantine_root=root / "quarantine", metadata_root=metadata,
                    run_id=f"e2e-{dataset}", now=datetime(2026, 8, 11, tzinfo=timezone.utc),
                )
            self.assertEqual(results["prices"].accepted_rows, 2)
            self.assertEqual(results["prices"].quarantined_rows, 1)
            quarantine = root / "quarantine/source=Test-Provider/run=e2e-prices.jsonl"
            rejected = json.loads(quarantine.read_text().strip())
            self.assertEqual(rejected["raw_payload"]["symbol"], "REJECT")

            database = root / "marketforge.duckdb"
            profiles = root / "profiles"
            profiles.mkdir()
            (profiles / "profiles.yml").write_text(
                "marketforge:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n"
                f"      path: '{database}'\n      schema: main\n      threads: 2\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [DBT, "build", "--project-dir", str(ROOT / "dbt"),
                 "--profiles-dir", str(profiles), "--vars",
                 json.dumps({"raw_root": str(raw), "metadata_root": str(metadata)}),
                 "--no-use-colors"],
                cwd=ROOT, text=True, capture_output=True, check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            with duckdb.connect(str(database), read_only=True) as connection:
                raw_count = connection.execute("SELECT count(*) FROM raw_prices").fetchone()[0]
                mart = connection.execute("""SELECT symbol, trade_date, close, daily_return
                    FROM main_marts.mart_security_daily ORDER BY trade_date DESC LIMIT 1""").fetchone()
            self.assertEqual(raw_count, 2)
            self.assertEqual(mart[0:3], ("AAPL", datetime(2026, 8, 10).date(), 101.0))
            self.assertAlmostEqual(mart[3], 0.01)

            metadata_store = root / "operational.sqlite"
            with closing(sqlite3.connect(metadata_store)) as connection, connection:
                connection.execute("CREATE TABLE marker (value INTEGER)")
            client = TestClient(create_app(
                database=database, lineage_path=root / "lineage.json",
                metadata_store=metadata_store,
            ))
            security = client.get("/api/securities/AAPL")
            self.assertEqual(security.status_code, 200)
            self.assertEqual(security.json()["latest_price"], 101.0)
            history = client.get("/api/securities/AAPL/history?source=test-provider")
            self.assertEqual(history.status_code, 200)
            self.assertEqual(history.json()["data"][0]["trade_date"], "2026-08-10")
            pipeline = client.get("/api/pipeline/health")
            self.assertEqual(pipeline.status_code, 200)
            prices = next(row for row in pipeline.json()["data"] if row["dataset"] == "prices")
            self.assertEqual(prices["row_count"], 2)
            self.assertEqual(prices["quarantine_count"], 1)


if __name__ == "__main__":
    unittest.main()

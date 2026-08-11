import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from ingestion.loaders import run_backfill
from ingestion.sources.files import read_records


DBT = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
if not Path(DBT).is_file():
    DBT = None
ROOT = Path(__file__).parents[2]
EXPECTED = json.loads((ROOT / "tests/fixtures/analytics/expected_metrics.json").read_text())


def price(symbol, day, close, volume, record):
    return {
        "symbol": symbol, "date": day.isoformat(), "open": close,
        "high": close + 1, "low": close - 1, "close": close, "volume": volume,
        "source_record_id": record,
    }


@unittest.skipUnless(DBT, "dbt is not installed in this environment")
class AnalyticalCorrectnessTests(unittest.TestCase):
    def test_dbt_metrics_equal_hand_calculated_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            raw = temp / "raw"
            start = date(2026, 7, 20)
            prices = [
                price("AAPL", start + timedelta(days=index), 100 + index, 200 if index == 20 else 100,
                      f"AAPL:{index}")
                for index in range(21)
            ]
            prices.extend((
                price("MSFT", start + timedelta(days=19), 200, 100, "MSFT:19"),
                price("MSFT", start + timedelta(days=20), 190, 100, "MSFT:20"),
                price("XOM", start + timedelta(days=19), 50, 100, "XOM:19"),
                price("XOM", start + timedelta(days=20), 50, 100, "XOM:20"),
            ))
            options = {
                "source": "test-provider", "raw_root": raw,
                "quarantine_root": temp / "quarantine", "metadata_root": temp / "metadata",
                "now": datetime(2026, 8, 11, tzinfo=timezone.utc),
            }
            run_backfill("prices", prices, run_id="analytics-prices", **options)
            for dataset in ("fundamentals", "earnings", "macro", "news"):
                records = read_records(str(ROOT / f"tests/fixtures/ci/{dataset}.jsonl"))
                run_backfill(dataset, records, run_id=f"analytics-{dataset}", **options)

            database = temp / "analytics.duckdb"
            profiles = temp / "profiles"
            profiles.mkdir()
            (profiles / "profiles.yml").write_text(
                "marketforge:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n"
                f"      path: '{database}'\n      schema: main\n      threads: 2\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [DBT, "build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(profiles),
                 "--vars", json.dumps({"raw_root": str(raw), "metadata_root": str(temp / "metadata")}),
                 "--no-use-colors"],
                cwd=ROOT, text=True, capture_output=True, check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            with duckdb.connect(str(database), read_only=True) as connection:
                security = connection.execute(
                    """SELECT daily_return, rolling_20d_return, rolling_20d_volatility,
                              annualized_20d_volatility, average_volume_20d, relative_volume,
                              return_observations_20d
                       FROM main_marts.mart_security_daily
                       WHERE symbol=? AND trade_date=?""",
                    [EXPECTED["symbol"], EXPECTED["as_of"]],
                ).fetchone()
                for actual, key in zip(security, (
                    "daily_return", "rolling_20d_return", "rolling_20d_volatility",
                    "annualized_20d_volatility", "average_volume_20d", "relative_volume",
                )):
                    self.assertAlmostEqual(actual, EXPECTED[key], places=12, msg=key)
                self.assertEqual(security[6], EXPECTED["return_observations_20d"])

                sector = connection.execute(
                    """SELECT sector_average_return FROM main_marts.mart_sector_daily
                       WHERE sector=? AND trade_date=?""",
                    [EXPECTED["sector"], EXPECTED["as_of"]],
                ).fetchone()[0]
                self.assertAlmostEqual(sector, EXPECTED["sector_average_return"], places=12)
                breadth = connection.execute(
                    """SELECT market_breadth, advancers, decliners, unchanged
                       FROM main_marts.mart_market_daily WHERE trade_date=?""",
                    [EXPECTED["as_of"]],
                ).fetchone()
                self.assertAlmostEqual(breadth[0], EXPECTED["market_breadth"], places=12)
                self.assertEqual(breadth[1:], (
                    EXPECTED["advancers"], EXPECTED["decliners"], EXPECTED["unchanged"]
                ))


if __name__ == "__main__":
    unittest.main()

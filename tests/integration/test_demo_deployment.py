import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import create_app
from scripts.build_demo_snapshot import _build_live, build_snapshot


ROOT = Path(__file__).parents[2]


class DemoDeploymentTests(unittest.TestCase):
    def test_curated_snapshot_supports_public_dashboard_without_live_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "generated"
            result = build_snapshot(output)
            app = create_app(
                database=Path(result["database"]), metadata_store=Path(result["metadata"]),
                lineage_path=Path(result["lineage"]), raw_root=Path(result["raw_root"]),
                project_root=output, budget_path=ROOT / "config/resource_budget.yaml",
                benchmarks_path=Path(result["benchmarks"]),
                quarantine_root=output / "quarantine",
            )
            with TestClient(app) as client:
                self.assertEqual(client.get("/health/ready").status_code, 200)
                self.assertEqual(client.get("/api/securities").json()["meta"]["returned"], 3)
                self.assertEqual(
                    client.get("/api/securities/AAPL/history").json()["meta"]["returned"], 30
                )
                self.assertEqual(len(client.get("/api/sectors").json()), 2)
                self.assertEqual(client.get("/api/pipeline/runs").json()[0]["status"], "success")
                self.assertEqual(client.get("/api/system/benchmarks").json()["environment"],
                                 "full-local-lake-measurement")

    def test_live_snapshot_exposes_all_provider_datasets(self):
        today = date(2026, 8, 12)
        records = {
            "prices": ("tiingo", [{"symbol": "AAPL", "date": "2026-08-12", "open": 100,
                "high": 102, "low": 99, "close": 101, "volume": 10,
                "source_record_id": "AAPL:2026-08-12"}]),
            "fundamentals": ("sec-edgar", [{"symbol": "AAPL", "metric_name": "Assets",
                "period_start": None, "period_end": "2026-06-30", "period_type": "Q2",
                "filed_at": "2026-08-01T00:00:00Z", "value": 10, "unit": "USD",
                "currency": "USD", "source_record_id": "sec:assets"}]),
            "earnings": ("businessquant", [{"symbol": "AAPL",
                "event_timestamp": "2026-08-12T00:00:00Z", "fiscal_period_end": "2026-06-30",
                "event_status": "REPORTED", "eps_estimate": 1.0, "eps_actual": 1.1,
                "source_record_id": "bq:eps"}]),
            "macro": ("fred", [{"series_id": "CPIAUCSL", "observation_date": "2026-07-01",
                "released_at": None, "value": 100, "unit": "INDEX", "frequency": "MONTHLY",
                "source_record_id": "fred:cpi"}]),
            "news": ("newsapi", [{"event_timestamp": "2026-08-12T00:00:00Z",
                "headline": "Market update", "url": "https://example.com/story",
                "publisher": "Example", "source_record_id": "news:story"}]),
        }
        with tempfile.TemporaryDirectory() as directory, patch(
            "scripts.build_demo_snapshot._live_records", return_value=records
        ):
            result = _build_live(Path(directory) / "generated", today=today)
            app = create_app(
                database=Path(result["database"]), metadata_store=Path(result["metadata"]),
                lineage_path=Path(result["lineage"]), raw_root=Path(result["raw_root"]),
                project_root=Path(directory), budget_path=ROOT / "config/resource_budget.yaml",
                benchmarks_path=Path(result["benchmarks"]),
            )
            with TestClient(app) as client:
                datasets = client.get("/api/pipeline/health").json()["data"]
                self.assertEqual({row["dataset"] for row in datasets}, {
                    "prices", "fundamentals", "earnings", "macro", "news",
                })
                detail = client.get("/api/securities/AAPL").json()
                self.assertEqual(detail["available_fundamental_metrics"], 1)
                self.assertAlmostEqual(detail["latest_eps_actual"], 1.1)


if __name__ == "__main__":
    unittest.main()

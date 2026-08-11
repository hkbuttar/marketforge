import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from backend.main import create_app
from scripts.build_demo_snapshot import build_snapshot


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


if __name__ == "__main__":
    unittest.main()

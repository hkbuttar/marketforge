import hashlib
import tempfile
import unittest
from pathlib import Path

import duckdb

from warehouse.time_travel import ReproductionError, create_catalog, reproduction_plan


class TimeTravelTests(unittest.TestCase):
    def test_exact_inputs_create_metadata_only_historical_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "data/raw/prices/year=2025/month=01/part-one.parquet"
            artifact.parent.mkdir(parents=True)
            with duckdb.connect() as connection:
                connection.execute("COPY (SELECT 'AAPL' symbol, DATE '2025-01-02' date) TO ? (FORMAT PARQUET)", [str(artifact)])
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            manifest = {"build_id": "a" * 64, "datasets": ["prices"], "parameters": {},
                "git_commit": "commit", "git_worktree": "clean", "dbt_code_hash": "code",
                "dbt_run_id": "dbt", "source_partitions": [{"dataset": "prices",
                    "partition": "year=2025/month=01", "artifact": str(artifact.relative_to(root)),
                    "content_hash": digest, "row_count": 1, "pipeline_run_id": "one"}]}
            plan = reproduction_plan(manifest, repo_root=root)
            self.assertTrue(plan["inputs_ready"])
            target = root / "history/build.duckdb"
            result = create_catalog(plan, target)
            self.assertEqual(result["dataset_rows"], {"prices": 1})
            with duckdb.connect(str(target), read_only=True) as connection:
                self.assertEqual(connection.execute("SELECT symbol FROM raw.prices").fetchone()[0], "AAPL")

    def test_changed_input_refuses_catalog(self):
        plan = {"inputs_ready": False, "artifacts": [{"artifact": "missing", "available": False,
                "hash_valid": False}], "build_id": "a" * 64, "required_code": {}}
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ReproductionError):
                create_catalog(plan, Path(directory) / "history.duckdb")


if __name__ == "__main__":
    unittest.main()

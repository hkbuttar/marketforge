import tempfile
import unittest
from pathlib import Path

from ingestion.loaders import run_backfill
from observability.builds import create_build_manifest, verify_build_manifest


class ReproducibleBuildTests(unittest.TestCase):
    def test_build_id_is_stable_and_source_tampering_is_detected(self):
        repo = Path(__file__).parents[2]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_backfill(
                "prices", [{
                    "symbol": "AAPL", "date": "2026-08-10", "open": 100,
                    "high": 102, "low": 99, "close": 101, "volume": 1000,
                    "source_record_id": "AAPL:2026-08-10",
                }], source="tiingo", raw_root=root / "raw",
                quarantine_root=root / "quarantine", metadata_root=root / "runs",
                run_id="source-run",
            )
            first, target = create_build_manifest(
                repo_root=repo, raw_root=root / "raw", output_root=root / "builds",
                datasets=["prices"], parameters={"universe": "test"},
            )
            second, second_target = create_build_manifest(
                repo_root=repo, raw_root=root / "raw", output_root=root / "builds",
                datasets=["prices"], parameters={"universe": "test"},
            )
            self.assertEqual(first["build_id"], second["build_id"])
            self.assertEqual(target, second_target)
            self.assertFalse(verify_build_manifest(target, repo))
            self.assertEqual(first["source_partitions"][0]["pipeline_run_id"], "source-run")

            artifact = Path(first["source_partitions"][0]["artifact"])
            with artifact.open("ab") as stream:
                stream.write(b"tampered")
            errors = verify_build_manifest(target, repo)
            self.assertTrue(any("hash changed" in error for error in errors))


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[2]
REQUIRED = {
    "source contract correctness", "normalization correctness", "idempotency",
    "deduplication", "atomic-write recovery", "checkpoint consistency",
    "late-arriving data handling", "schema evolution", "backfill overlap",
    "data-quality checks", "reconciliation invariants", "dbt model correctness",
    "derived-metric correctness", "freshness calculation", "storage guardrails",
    "compaction equivalence", "FastAPI responses", "end-to-end pipeline",
    "injected process failures",
}


class ValidationManifestTests(unittest.TestCase):
    def test_every_required_category_points_to_existing_tests(self):
        manifest = json.loads((ROOT / "tests/validation_manifest.json").read_text())
        self.assertEqual(set(manifest), REQUIRED)
        for category, paths in manifest.items():
            with self.subTest(category=category):
                self.assertTrue(paths)
                for path in paths:
                    target = ROOT / path
                    self.assertTrue(target.is_file(), target)
                    self.assertIn("def test_", target.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

import tempfile
import unittest
from pathlib import Path

from scripts.check_robustness import FEATURES, assess


class RobustnessReadinessTests(unittest.TestCase):
    def test_checklist_matches_step_57(self):
        self.assertEqual(len(FEATURES), 15)
        self.assertEqual(
            set(FEATURES),
            {
                "multiple sources", "late-arriving data", "schema evolution", "quarantine",
                "reconciliation", "storage benchmarks", "partition benchmarks", "compaction",
                "resource guardrails", "failure injection", "dataset manifests", "full lineage",
                "CI", "public demo", "StreamAlpha adapter",
            },
        )

    def test_repository_passes_robustness_gate(self):
        report = assess(Path(__file__).parents[2])
        self.assertEqual(report["status"], "READY")
        self.assertEqual(report["passed"], 15)

    def test_missing_artifacts_fail_with_exact_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            report = assess(Path(directory))
        self.assertEqual(report["status"], "NOT_READY")
        self.assertEqual(report["passed"], 0)
        self.assertTrue(all(check["missing"] for check in report["checks"]))


if __name__ == "__main__":
    unittest.main()

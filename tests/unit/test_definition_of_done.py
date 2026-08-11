import tempfile
import unittest
from pathlib import Path

from scripts.check_definition_of_done import CONDITIONS, assess


class DefinitionOfDoneTests(unittest.TestCase):
    def test_definition_contains_eighteen_numbered_conditions(self):
        report = assess(Path(__file__).parents[2])
        self.assertEqual(len(CONDITIONS), 18)
        self.assertEqual([check["number"] for check in report["checks"]], list(range(1, 19)))

    def test_repository_has_evidence_for_every_condition(self):
        report = assess(Path(__file__).parents[2])
        self.assertEqual(report["status"], "COMPLETE")
        self.assertEqual(report["passed"], 18)

    def test_missing_repository_reports_every_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            report = assess(Path(directory))
        self.assertEqual(report["status"], "INCOMPLETE")
        self.assertEqual(report["passed"], 0)
        self.assertTrue(all(check["missing"] for check in report["checks"]))


if __name__ == "__main__":
    unittest.main()

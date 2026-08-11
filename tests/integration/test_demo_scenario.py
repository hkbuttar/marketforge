import tempfile
import unittest
from pathlib import Path

from scripts.demo import run_demo


class DemoScenarioTests(unittest.TestCase):
    def test_all_state_transitions_preserve_declared_invariants(self):
        with tempfile.TemporaryDirectory() as directory:
            result = run_demo(Path(directory), transform=False)
        self.assertTrue(result["success"])
        self.assertTrue(all(result["invariants"].values()))
        events = {event["event"]: event for event in result["events"]}
        self.assertEqual(events["malformed_fundamental"]["status"], "degraded")
        self.assertEqual(events["late_aug8_correction"]["checkpoint"], "2026-08-11")
        self.assertEqual(events["idempotent_aug11_replay"]["accepted_rows"], 0)
        self.assertEqual(events["crash_and_recovery"]["checkpoint_after_retry"], "2026-08-12")


if __name__ == "__main__":
    unittest.main()

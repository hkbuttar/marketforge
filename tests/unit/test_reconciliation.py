import unittest

from quality.reconciliation import reconcile_run


class ReconciliationTests(unittest.TestCase):
    def test_balanced_run_passes(self):
        result = reconcile_run(
            fetched=10, accepted=6, rejected=1, deduplicated=3,
            written=6, pre_write_rows=20, post_write_rows=26,
        )
        self.assertEqual(result.status, "passed")
        self.assertFalse(result.discrepancies)

    def test_unexplained_loss_and_partition_mismatch_are_audited(self):
        result = reconcile_run(
            fetched=10, accepted=6, rejected=1, deduplicated=2,
            written=5, pre_write_rows=20, post_write_rows=24,
        )
        self.assertEqual(result.status, "failed")
        self.assertEqual(len(result.discrepancies), 3)


if __name__ == "__main__":
    unittest.main()

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from observability.resource_guardrails import ResourceLimitExceeded, assess_storage, enforce_storage
from scripts.cleanup import candidates


BUDGET = """project_limits:\n  raw_data_gb: 2\nstorage:\n  warning_gb: 1\n  hard_limit_gb: 3\n  minimum_free_gb: 1\n  cleanup_retention_days: 14\n"""


class ResourceGuardrailTests(unittest.TestCase):
    def test_safe_warning_and_hard_limit_assessments(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            budget = root / "budget.yml"
            budget.write_text(BUDGET)
            usage = type("usage", (), {"free": 10_000_000_000})()
            with patch("observability.resource_guardrails.shutil.disk_usage", return_value=usage):
                self.assertEqual(assess_storage(project_root=root, raw_root=root / "raw",
                    projected_write_bytes=10, budget_path=budget).status, "safe")
                self.assertEqual(assess_storage(project_root=root, raw_root=root / "raw",
                    projected_write_bytes=1_500_000_000, budget_path=budget).status, "warning")
                with self.assertRaises(ResourceLimitExceeded):
                    enforce_storage(project_root=root, raw_root=root / "raw",
                        projected_write_bytes=2_500_000_000, budget_path=budget)

    def test_cleanup_never_selects_canonical_raw_or_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "data/raw/prices/year=2025/month=01/part.parquet"
            expired = root / "data/raw/.tmp/run/part.writing"
            canonical.parent.mkdir(parents=True)
            expired.parent.mkdir(parents=True)
            canonical.write_bytes(b"keep")
            expired.write_bytes(b"remove")
            old = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
            os.utime(canonical, (old, old))
            os.utime(expired, (old, old))
            selected = candidates(root, older_than=datetime.now(timezone.utc) - timedelta(days=14))
            self.assertEqual(selected, [expired])
            self.assertNotIn(canonical, selected)


if __name__ == "__main__":
    unittest.main()

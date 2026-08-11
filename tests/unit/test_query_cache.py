import tempfile
import time
import unittest
from pathlib import Path

from backend.services.cache import QueryCache
from backend.services.query import QueryService


class QueryCacheTests(unittest.TestCase):
    def test_ttl_hits_copy_values_and_expire(self):
        cache = QueryCache(ttl_seconds=0.01, max_entries=2)
        calls = []
        first = cache.get_or_compute("key", lambda: calls.append(1) or [1])
        first.append(2)
        self.assertEqual(cache.get_or_compute("key", lambda: calls.append(1)), [1])
        self.assertEqual(len(calls), 1)
        time.sleep(0.02)
        cache.get_or_compute("key", lambda: calls.append(1) or [3])
        self.assertEqual(len(calls), 2)

    def test_lru_capacity_is_bounded(self):
        cache = QueryCache(max_entries=2)
        for key in range(3):
            cache.get_or_compute(key, lambda key=key: key)
        self.assertEqual(cache.stats()["entries"], 2)
        self.assertEqual(cache.stats()["evictions"], 1)

    def test_dataset_build_version_changes_cache_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "marts.duckdb"
            database.write_bytes(b"version")
            builds = root / "builds"
            builds.mkdir()
            service = QueryService(database, root / "lineage.json", builds_root=builds)
            calls = []
            compute = lambda: calls.append(1) or "result"
            service._cached("test", (), compute)
            service._cached("test", (), compute)
            (builds / "new-build.json").write_text("{}")
            service._cached("test", (), compute)
            self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()

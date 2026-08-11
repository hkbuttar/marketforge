import importlib.util
import tempfile
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "benchmark", Path(__file__).parents[2] / "scripts" / "benchmark.py"
)
benchmark = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(benchmark)


class PathSizeTests(unittest.TestCase):
    def test_recursive_size_ignores_missing_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "nested").mkdir()
            (root / "a.bin").write_bytes(b"123")
            (root / "nested" / "b.bin").write_bytes(b"4567")
            self.assertEqual(benchmark.paths_size([root, root / "missing"]), 7)

    def test_file_size(self):
        with tempfile.NamedTemporaryFile() as stream:
            stream.write(b"marketforge")
            stream.flush()
            self.assertEqual(benchmark.path_size(Path(stream.name)), 11)


if __name__ == "__main__":
    unittest.main()

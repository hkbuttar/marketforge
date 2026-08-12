import tempfile
import unittest
from pathlib import Path

from scripts.update_tiingo import load_universe, shard


class TiingoDailyTests(unittest.TestCase):
    def test_universe_ignores_comments_and_blank_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "universe.txt"
            path.write_text("# note\naapl\n\nMSFT\n", encoding="utf-8")
            self.assertEqual(load_universe(path), ["AAPL", "MSFT"])

    def test_hundred_symbols_split_into_two_hourly_safe_shards(self):
        symbols = [f"S{number}" for number in range(100)]
        self.assertEqual(shard(symbols, 0, 50), symbols[:50])
        self.assertEqual(shard(symbols, 1, 50), symbols[50:])
        self.assertEqual(shard(symbols, 2, 50), [])

    def test_invalid_shard_parameters_fail(self):
        with self.assertRaisesRegex(ValueError, "positive"):
            shard(["AAPL"], 0, 0)
        with self.assertRaisesRegex(ValueError, "non-negative"):
            shard(["AAPL"], -1, 50)


if __name__ == "__main__":
    unittest.main()

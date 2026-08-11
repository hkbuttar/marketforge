import json
import os
import unittest
from datetime import date
from unittest.mock import patch

from ingestion.sources.tiingo import TiingoError, fetch_prices


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode()


class TiingoSourceTests(unittest.TestCase):
    def test_maps_eod_response_to_price_contract_shape(self):
        payload = [{
            "date": "2026-08-10T00:00:00.000Z", "open": 100, "high": 102,
            "low": 99, "close": 101, "volume": 1234, "adjClose": 100.5,
        }]
        with patch("ingestion.sources.tiingo.urlopen", return_value=Response(payload)) as call:
            rows = fetch_prices(
                ["aapl"], start=date(2026, 8, 10), end=date(2026, 8, 10), api_key="secret"
            )
        self.assertEqual(rows[0], {
            "symbol": "AAPL", "date": "2026-08-10", "open": 100, "high": 102,
            "low": 99, "close": 101, "volume": 1234,
            "source_record_id": "AAPL:2026-08-10",
        })
        request = call.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Token secret")
        self.assertNotIn("secret", request.full_url)

    def test_requires_environment_token(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(TiingoError, "TIINGO_API_KEY"):
                fetch_prices(["AAPL"], start=date(2026, 8, 10), end=date(2026, 8, 10))

    def test_rejects_unsafe_ticker_before_network_request(self):
        with self.assertRaisesRegex(ValueError, "invalid ticker"):
            fetch_prices(["../AAPL"], start=date(2026, 8, 10), end=date(2026, 8, 10), api_key="x")


if __name__ == "__main__":
    unittest.main()

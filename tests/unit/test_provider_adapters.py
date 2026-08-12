import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from ingestion.sources.businessquant import _period_end, fetch_earnings
from ingestion.sources.newsapi import fetch_news
from ingestion.sources.sec_edgar import fetch_fundamentals


class ProviderAdapterTests(unittest.TestCase):
    def test_businessquant_maps_quarter_without_inventing_release_time(self):
        payload = {"data": [{"dimension": "quarter", "estimates": [{
            "period": "Q2 26", "data_type": "reported", "value_estimate": 1.2,
            "value_reported": 1.3,
        }]}]}
        observed = datetime(2026, 8, 12, tzinfo=timezone.utc)
        with patch("ingestion.sources.businessquant.get_json", return_value=payload):
            row = fetch_earnings("aapl", api_key="key", observed_at=observed)[0]
        self.assertEqual(row["fiscal_period_end"], "2026-06-30")
        self.assertEqual(row["event_timestamp"], observed.isoformat())
        self.assertEqual(row["event_status"], "REPORTED")
        self.assertEqual(_period_end("Q4 24"), "2024-12-31")

    def test_sec_deduplicates_same_accession_fact(self):
        fact = {"units": {"USD": [{
            "start": "2026-01-01", "end": "2026-03-31", "val": 10,
            "accn": "x", "fp": "Q1", "form": "10-Q", "filed": "2026-05-01",
        }] * 2}}
        with patch("ingestion.sources.sec_edgar.get_json", return_value={
            "facts": {"us-gaap": {"NetIncomeLoss": fact}}
        }):
            rows = fetch_fundamentals("AAPL", "320193", user_agent="MarketForge a@b.com")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["currency"], "USD")

    def test_news_retains_metadata_only(self):
        payload = {"status": "ok", "articles": [{
            "publishedAt": "2026-08-12T12:00:00Z", "title": "Example",
            "url": "https://example.com/a", "source": {"name": "Example News"},
            "description": "not retained", "content": "not retained",
        }]}
        with patch("ingestion.sources.newsapi.get_json", return_value=payload):
            row = fetch_news("test", start=datetime.now().date(),
                             end=datetime.now().date(), api_key="key")[0]
        self.assertEqual(set(row), {
            "event_timestamp", "headline", "url", "publisher", "source_record_id",
        })


if __name__ == "__main__":
    unittest.main()

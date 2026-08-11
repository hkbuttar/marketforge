import re
import unittest
from pathlib import Path


SCHEMA = (Path(__file__).parents[2] / "warehouse" / "duckdb" / "init.sql").read_text()


def table_body(name: str) -> str:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS canonical\.{name}\s*\((.*?)\n\);",
        SCHEMA,
        re.DOTALL,
    )
    if not match:
        raise AssertionError(f"canonical.{name} is missing")
    return match.group(1)


class CanonicalSchemaTests(unittest.TestCase):
    def test_all_planned_entities_exist(self):
        entities = {
            "security",
            "trading_day",
            "price_bar",
            "fundamental_observation",
            "earnings_event",
            "macro_observation",
            "news_event",
            "sector",
            "industry",
        }
        for entity in entities:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS canonical.{entity}", SCHEMA)

    def test_security_facts_use_durable_security_id(self):
        for entity in ("price_bar", "fundamental_observation", "earnings_event"):
            body = table_body(entity)
            self.assertRegex(body, r"security_id UUID NOT NULL REFERENCES canonical\.security")
            self.assertNotRegex(body, r"\bsymbol\b")

    def test_sourced_facts_separate_event_and_ingestion_time(self):
        event_columns = {
            "price_bar": "trade_date",
            "fundamental_observation": "period_end",
            "earnings_event": "event_timestamp",
            "macro_observation": "observation_date",
            "news_event": "event_timestamp",
        }
        for entity, event_column in event_columns.items():
            body = table_body(entity)
            self.assertRegex(body, rf"\b{event_column}\b")
            self.assertRegex(body, r"\bingested_at TIMESTAMPTZ NOT NULL\b")
            self.assertRegex(body, r"\bsource_record_id VARCHAR NOT NULL\b")

    def test_symbol_history_is_effective_dated(self):
        body = table_body("security_identifier")
        for column in ("identifier_value", "valid_from", "valid_to", "security_id"):
            self.assertRegex(body, rf"\b{column}\b")


if __name__ == "__main__":
    unittest.main()

import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("dagster"), "Dagster is not installed")
class OrchestrationDefinitionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from orchestration.definitions import defs

        cls.repository = defs.get_repository_def()
        cls.graph = cls.repository.asset_graph

    def test_requested_jobs_exist(self):
        names = {job.name for job in self.repository.get_all_jobs()}
        self.assertTrue({
            "daily_incremental", "historical_backfill", "quality_validation", "rebuild_marts"
        }.issubset(names))

    def test_schedules_are_opt_in_and_timezone_explicit(self):
        schedules = self.repository.schedule_defs
        self.assertEqual({schedule.name for schedule in schedules}, {
            "prices_after_close_schedule", "macro_daily_check_schedule",
            "fundamentals_daily_check_schedule", "earnings_daily_schedule",
            "news_periodic_schedule", "daily_publish_schedule",
        })
        for schedule in schedules:
            self.assertEqual(schedule.execution_timezone, "America/Chicago")
            self.assertEqual(schedule.default_status.value, "STOPPED")

    def test_asset_graph_has_quality_gated_serving_lineage(self):
        from dagster import AssetKey

        keys = self.graph.get_all_asset_keys()
        self.assertEqual(len(keys), 21)
        self.assertIn(AssetKey("raw_prices"), self.graph.get(AssetKey("stg_prices")).parent_keys)
        self.assertIn(AssetKey("stg_prices"), self.graph.get(AssetKey("int_daily_returns")).parent_keys)
        self.assertIn(
            AssetKey("int_daily_returns"),
            self.graph.get(AssetKey("int_rolling_volatility")).parent_keys,
        )
        self.assertIn(
            AssetKey("int_rolling_volatility"),
            self.graph.get(AssetKey("mart_security_daily")).parent_keys,
        )
        self.assertIn(AssetKey("quality_gate"), self.graph.get(AssetKey("api_ready")).parent_keys)

    def test_raw_assets_have_freshness_and_retry_policies(self):
        from dagster import AssetKey

        for dataset in ("prices", "fundamentals", "earnings", "macro", "news"):
            node = self.graph.get(AssetKey(f"raw_{dataset}"))
            self.assertIsNotNone(node.freshness_policy)
            self.assertEqual(node.assets_def.op.retry_policy.max_retries, 2)


if __name__ == "__main__":
    unittest.main()

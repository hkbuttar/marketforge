.PHONY: daily quality backfill dagster

daily:
	.venv/bin/dagster job execute -m orchestration.definitions -j daily_incremental

quality:
	.venv/bin/dagster job execute -m orchestration.definitions -j quality_validation

backfill:
	.venv/bin/dagster job execute -m orchestration.definitions -j historical_backfill

dagster:
	DAGSTER_HOME="$(CURDIR)/config/dagster" .venv/bin/dagster dev -m orchestration.definitions

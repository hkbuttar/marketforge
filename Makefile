.PHONY: daily quality backfill dagster demo-image demo-container

daily:
	.venv/bin/dagster job execute -m orchestration.definitions -j daily_incremental

quality:
	.venv/bin/dagster job execute -m orchestration.definitions -j quality_validation

backfill:
	.venv/bin/dagster job execute -m orchestration.definitions -j historical_backfill

dagster:
	DAGSTER_HOME="$(CURDIR)/config/dagster" .venv/bin/dagster dev -m orchestration.definitions

demo-image:
	docker build --tag marketforge-api:local .

demo-container:
	docker run --rm --publish 8000:8000 --name marketforge-api marketforge-api:local

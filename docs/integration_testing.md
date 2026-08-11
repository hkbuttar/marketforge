# Integration testing

The acceptance-path integration test uses deterministic five-domain fixtures and
proves one price record across the complete platform boundary:

```text
JSONL source -> contract ingestion -> Parquet -> DuckDB raw view
-> dbt staging/intermediate/mart -> FastAPI response
```

The same prices batch contains a deliberately invalid OHLC row. The test verifies
that it reaches structured quarantine, does not enter raw Parquet or the mart, and
is reflected in pipeline-health metadata. The accepted AAPL close and calculated
daily return are asserted independently in DuckDB and through FastAPI.

Run the acceptance path alone with:

```bash
python -m unittest tests.integration.test_end_to_end_platform -v
```

All paths use a temporary directory and synthetic records. The test neither reads
the developer's lake nor contacts Tiingo or StreamAlpha.

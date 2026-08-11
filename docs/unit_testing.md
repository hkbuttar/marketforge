# Unit testing

The unit suite uses tiny synthetic inputs and isolates deterministic boundaries:

| Requirement | Coverage |
|---|---|
| Schema normalization and contract validation | `test_source_contracts.py` |
| Checkpoint calculations | `test_ingestion_calculations.py` |
| Deterministic hashing | `test_ingestion_calculations.py` |
| Deduplication and conflict detection | `test_ingestion_calculations.py` |
| Partition path generation | `test_ingestion_calculations.py` |
| Freshness rules | `test_freshness.py` |
| Reconciliation arithmetic | `test_reconciliation.py` |
| Storage-budget calculations | `test_resource_guardrails.py` |

Run only fast unit tests with:

```bash
python -m unittest discover -s tests/unit -v
```

Filesystem/database lifecycle and cross-component behavior intentionally remain in
the integration and failure suites rather than being mislabeled as unit tests.

# Synthetic failure dataset

`tests/fixtures/failures/prices.json` is a versioned corpus of eleven deliberately
hostile provider cases. Each case declares its expected stage and diagnostic:

- duplicate row
- missing symbol
- negative volume
- high below low
- wrong date type
- late-arriving record
- unknown security
- additive and subtractive schema drift
- nonnumeric string in a numeric field
- empty API response

Contract failures are either quarantined row-by-row or fail the batch when an
entire required column disappears. Late data is accepted and audited without
moving a checkpoint backward. Unknown securities pass raw source preservation but
fail the separate universe-resolution quality gate. Empty responses create no raw
artifact and do not advance state.

The required CI suite discovers `tests/failure/test_synthetic_failure_dataset.py`,
so adding a corpus case without updating its expected behavior fails CI.

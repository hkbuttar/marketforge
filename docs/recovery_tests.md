# Recovery tests

Recovery drills extend failure injection through a successful restart. Every drill
creates a structured record containing the initial state, injected failure,
observed behavior, retry action, final state, named invariants, and overall
recovery status.

Covered restart paths:

| Failure | Retry | Final invariants |
| --- | --- | --- |
| Provider HTTP 503 | Repeat the bounded request after service recovery | One acknowledged row, no duplicate |
| Invalid provider value | Correct payload and use a new run | Bad row remains quarantined; corrected row appears once |
| Disk exhaustion before write | Free space and retry the same run ID | No partial canonical file; row acknowledged once |
| Crash after promotion before checkpoint | Replay the same provider row | Existing row deduplicates; checkpoint advances correctly |
| dbt nonzero exit | Correct transformation and rerun dbt | Raw data remains queryable; retry succeeds |

The earlier atomic-write suite additionally restarts at every boundary before and
after temporary write, validation, final promotion, manifest creation, and
checkpoint advancement.

Recovery records are JSON and can be written with
`observability.recovery.write_recovery_record`. Tests store them in isolated
temporary roots so drills cannot alter the real lake. A recovery is successful
only when every named invariant is true.

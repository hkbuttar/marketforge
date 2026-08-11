# CI/CD

Every push and pull request runs three independent jobs:

- Python lint, unit, contract, FastAPI, failure, and integration tests
- dbt parse plus a full build/test against five tiny deterministic JSONL fixtures
- TypeScript compilation and a production Vite build using `npm ci`

CI never downloads historical market data and requires no provider credentials.
Jobs have explicit timeouts, read-only repository permissions, dependency caches,
and concurrency cancellation for superseded commits.

The separate `Live source smoke test` is manual only. It requires the repository
secret `TIINGO_API_KEY` plus explicit ticker/start/end inputs and performs one
bounded ephemeral ingestion. It is intentionally absent from the required push
gate so provider availability cannot make deterministic CI flaky.

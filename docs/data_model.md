# Canonical data model

This model is source-neutral. Ingestion adapters may use source-specific names,
but accepted records must resolve to these entities and keys before downstream
transformation.

## Identity rules

- `security_id` is a generated UUID that represents an economic instrument. It
  never changes when a ticker changes and is the only security join key used by
  facts.
- Symbols and vendor identifiers live in `security_identifier` with effective
  date bounds. They are attributes used to resolve a source record, not durable
  fact-table keys.
- A null `valid_to` means that an identifier remains current. Effective ranges
  use inclusive `valid_from` and exclusive `valid_to` semantics.
- Every sourced fact retains `source` and `source_record_id`. Their combination
  is unique within an entity and is the ingestion idempotency key.
- Natural-grain uniqueness is also enforced where it is unambiguous. A provider
  may publish more than one version of a logical observation, so source identity
  is retained even when natural keys exist.

## Time rules

`ingested_at` is always the UTC time at which MarketForge durably accepted a
record. It is never substituted for event time.

| Entity | Event/knowledge time | Grain and primary key |
| --- | --- | --- |
| `sector` | N/A | One classification, `sector_id` |
| `industry` | N/A | One classification, `industry_id` |
| `security` | N/A | One durable instrument, `security_id` |
| `security_identifier` | `valid_from`, `valid_to` | One effective identifier assignment, `security_identifier_id` |
| `trading_day` | `trade_date` | One exchange calendar date, (`exchange_code`, `trade_date`) |
| `price_bar` | `trade_date` | One daily bar per security and source, `price_bar_id` |
| `fundamental_observation` | `period_end`; `filed_at` is when it became public | One reported metric/version, `fundamental_observation_id` |
| `earnings_event` | `event_timestamp` | One source earnings event, `earnings_event_id` |
| `macro_observation` | `observation_date`; `released_at` is knowledge time | One series observation/vintage, `macro_observation_id` |
| `news_event` | `event_timestamp` | One source news item, `news_event_id` |
| `news_event_security` | inherited from `news_event` | One event-to-security relation, (`news_event_id`, `security_id`) |

All timestamps are stored as `TIMESTAMPTZ` and normalized to UTC. Dates represent
calendar concepts and do not receive an invented time zone. `fundamental_observation`
and `macro_observation` preserve both the period being described and the time the
value became knowable, preventing look-ahead joins. Late records therefore have
event/knowledge times earlier than `ingested_at` without changing their meaning.

## Classification and identifier history

The first version stores the current sector and industry on `security`. Historical
classification membership will become its own effective-dated bridge if a source
with reliable classification history is introduced. Identifier history is modeled
now because mutable symbols are already a known join hazard.

## Reproducibility

Facts are append-oriented and carry source identity plus ingestion time. Corrections
arrive as new source records or are rebuilt from immutable raw inputs; pipelines do
not silently replace the event time with load time. `created_at` on reference data
is operational metadata and has no analytical event-time meaning.

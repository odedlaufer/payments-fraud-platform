# ADR-003: One feature definition file, shared by Flink and dbt

- **Status:** Accepted
- **Date:** 2026-11-12

## Context

Every feature is computed twice: online by Flink, to make a decision within seconds, and offline by dbt, to build point-in-time training sets and to prove parity (FR-PAR1, FR-PAR2). Training-serving skew, the PRD's second problem statement, comes from those two implementations quietly disagreeing: one includes the current transaction and the other does not, one uses microseconds and the other milliseconds, one counts declined transactions and the other does not. The parity SLO (99.9% of decisions within tolerance) cannot hold unless both sides implement exactly the same definition.

## Decision

`features/definitions.yml` is the only place a feature is defined: name, version, entity, kind, window, inputs, type and meaning. The `payments-features` package loads it with strict validation, and both implementations are generated from it or checked against it, along with the generated catalogue `docs/feature-catalogue.md`.

The semantics both sides must implement are fixed in the file itself, and the loader refuses any change to them, so a change is always deliberate and visible:

| Rule | Value | Reason |
| --- | --- | --- |
| Event time | `event_ts`, the simulated authorization time | One timeline for features, labels and SCD2 joins |
| Precision | milliseconds | Flink's event-time attribute is `TIMESTAMP(3)`; offline truncates to match |
| Window | `[t - w, t]`, inclusive, current transaction and ties included | Flink `OVER ... RANGE ... PRECEDING AND CURRENT ROW` and Trino `RANGE` frames behave this way |
| Counted transactions | every transaction at creation, whatever its final status | Decisions are made before the status is known; card testing is about attempts |
| Amounts | USD-equivalent | The simulator generates amounts in USD-equivalent units |
| Watermark | 30 s | Later events go to `dlq.late_events` (FR-F6) |

A feature's meaning never changes in place: a different meaning is a new `version`. Each logged decision records the feature versions it used (FR-D3), so offline recomputation can always use the matching definition.

## Consequences

- Adding a feature is a change to one file; the Flink job, the dbt model and the catalogue follow from it.
- The feature-definition equality test (PRD 8.4) runs the same recorded events through both implementations and compares results feature by feature.
- `merchant_refund_rate_7d` needs the simulated time of a reversal, which the source does not record yet; a `status_event_ts` column is added with `make schema-evolve` when the feature is implemented.
- Some features are awkward to express in Flink SQL (previous card-present transaction, first-seen devices). The definition stays declarative either way; the implementation may use a UDF or a different construct as long as the parity test passes.

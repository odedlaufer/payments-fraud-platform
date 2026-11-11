# ADR-006: SCD2 history is rebuilt from the CDC log, not dbt snapshots

- **Status:** Accepted
- **Date:** 2026-11-11
- **Resolves:** PRD open question Q3
- **Amends:** FR-L2 ("dbt snapshots build SCD2")

## Context

FR-L2 asks for SCD2 history of customers, cards and merchants with `valid_from`, `valid_to` and `is_current`, and DQ-10 requires one current row per key with no overlapping or missing ranges. Q3 asked whether dbt-trino snapshots work cleanly on Iceberg.

A dbt snapshot compares the current state of a table with the previous snapshot each time it runs. That has two consequences here:

- **Changes between runs are lost.** A customer who changes email at 10:01 and again at 10:07, with snapshots every 15 minutes, has no 10:01 version. Account takeover (PRD section 3) is exactly a short burst of profile changes, so the lost versions are the ones that matter.
- **Versions carry run time, not change time,** unless the source has a reliable change timestamp, and even then only the last change per run survives.

The platform already has every change: bronze holds the full Debezium change log with each row's simulated `event_ts` (FR-F5).

## Decision

SCD2 tables (`lake.snapshots.{customers,cards,merchants}_scd2`) are dbt models rebuilt from the staged change log by the `scd2` macro:

- Events are sequenced by Kafka offset, which is commit order.
- A version starts at a key's first event and whenever a tracked column changes; tracked columns are every column except the key, timestamps and CDC metadata, discovered at run time so new columns are tracked automatically.
- `valid_from` is the change's simulated `event_ts`; `valid_to` is the next version's `valid_from`, or the delete time, or null for the current version.
- Hard deletes close the last version. The simulator stamps `event_ts` with the delete time immediately before deleting, in the same transaction, so the delete event's before-image records when it happened.

dbt snapshots are not used, so Q3's compatibility concern does not arise.

## Consequences

- Every version is captured, regardless of how often dbt runs, and history can be rebuilt at any time from bronze.
- `valid_from` is in simulated time, the same clock as transaction `event_ts`, so as-of joins (FR-L3) and point-in-time training sets (FR-PIT2) use one timeline.
- The models are full rebuilds (`table`). Dimension volumes are small; an incremental version is possible later if they grow.
- DQ-10 is enforced by generic tests: one current version per key, contiguous positive-length ranges, and the current version of every live key equals silver's current state.

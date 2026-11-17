# ADR-008: Label times are simulated, like every other event time

- **Status:** Accepted
- **Date:** 2026-11-17
- **Amends:** PRD FR-PIT1 ("label_known_at = chargeback commit time")

## Context

FR-PIT1 defines when a label becomes known as the chargeback's commit time, and FR-PIT2 builds training sets from it: transactions with `event_ts <= cutoff - maturity`, labels only if `label_known_at <= cutoff`.

Commit time is the real clock. Everything those comparisons involve runs on the simulated clock: `event_ts`, feature windows (ADR-003), SCD2 validity (ADR-006) and the 1-60 day chargeback delay (FR-SIM4). At 144x, a chargeback filed 30 simulated days after a transaction commits about 5 real hours later. Comparing a real-time `label_known_at` with a simulated cutoff would make the point-in-time rule meaningless: labels would look known almost immediately, which is exactly the leakage FR-PIT3 exists to prevent.

## Decision

`label_known_at` is the chargeback's simulated `event_ts` (when it was filed), on the same clock as every other event time. Maturity is `event_ts + 60 simulated days` (`label_maturity_days`, default 60, the maximum chargeback delay). `fct_labels.as_of_ts` is the simulation's current time, the latest transaction event time, and `is_mature` is relative to it.

Real commit times remain available in silver and bronze (`_commit_ts`) for latency and freshness SLOs, which are real-time by nature.

## Consequences

- One timeline for features, history, labels and cutoffs: point-in-time correctness is a comparison of like with like.
- In a real deployment, where the clocks coincide, this is the same as the PRD's definition.
- DQ-12 (`label_known_at >= event_ts`) is meaningful: both sides are simulated times.

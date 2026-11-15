# ADR-007: Customer features are computed on demand from online state

- **Status:** Accepted
- **Date:** 2026-11-15
- **Amends:** PRD FR-F2 ("compute the feature catalogue in Flink") for customer features

## Context

`new_device_24h` and `profile_change_24h` need three CDC topics at once: transactions, sessions (which device), and customers (which profile changes). In Flink SQL that is an event-time join across topics, which is only correct when the watermark covers the skew between them: an event that arrives behind the watermark is dropped.

The simulator runs at 144x, so the 30-second watermark of ADR-003 is 0.2 real seconds. Topics read in parallel routinely drift further apart than that, so a profile change would be dropped as late and `profile_change_24h` would read false exactly when an account is being taken over. A watermark wide enough to be safe (tens of real seconds) would hold every decision beyond its 3-second wait and turn most of them into `stale_features` reviews.

The card features do not have this problem: they come from one topic, in commit order.

## Decision

Customer features are lookups, not aggregations, so they are computed on demand when the decision is made, from online state that the feature sink keeps in Redis:

| Online state (maintained from CDC) | Feature, for a transaction at event time t |
| --- | --- |
| `first:{feature}:{customer}`: each device's first sighting, an atomic minimum | `new_device_24h`: the session's device was first seen in [t - 24h, t] |
| `chg:{feature}:{customer}`: sorted set of times a watched column changed | `profile_change_24h`: any change in [t - 24h, t] |
| `owner:{card}`, `session:{id}` | joins a transaction to its customer and device |

The sink derives keys and watched columns from `features/definitions.yml`, so a new feature of the same kind needs no new code. Every state update is idempotent (set, minimum, sorted-set add), so replays and duplicates are harmless, and the window queries are point-in-time by construction.

A transaction is ready to decide when its card snapshot has arrived and its customer context is known (card owner, and the session for an online transaction). Otherwise it waits like it waits for card features, and becomes `review` with reason `stale_features` at its deadline (FR-D4).

## Consequences

- Same parity discipline as Flink: `features.reference.customer_features` is the oracle; the on-demand code equals it on randomized scenarios through a real Redis (shuffled, duplicated delivery), and `make decisions-check` recomputes the customer features of every live decision from Postgres and the customers change log.
- Customer features add two Redis round trips per decision pass, batched across all pending transactions.
- If the simulator ran in real time, a Flink job with a 30-second watermark would be viable; this decision is about the accelerated clock as much as about the features. Merchant features (one topic, like card features) stay in Flink.
- Change times are kept for the window plus 30 days, pruned relative to the newest change recorded, so out-of-order replays never remove a change a decision could still need.

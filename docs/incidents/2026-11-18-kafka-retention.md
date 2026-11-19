# 2026-11-18: Kafka retention deleted the early history of the CDC topics

**Impact:** Redpanda's default retention (7 days, `cleanup.policy=delete`) removed the first week of every CDC topic: 72,161 of 300,221 transaction events, 10,074 session events, 2,836 of 3,183 card events, 2,310 customer and 358 merchant events. Bronze holds the complete history, so no data was lost. Found by `make offline-features-check`, whose reference read profile changes from Kafka and disagreed with the lake on 1,449 early transactions.

**Latent outage avoided:** the feature sink rebuilds Redis from the start of its topics, including each card's owner from `pg.public.cards`. With 89% of card events gone, a sink or Redis restart would have left most cards without a known owner: customer context never ready, nearly every decision a `stale_features` review. It had not restarted since the deletion.

## Root cause

Retention was never set. Several consumers assume a topic holds every event: the Flink feature job (replays from the earliest offset), the feature sink (rebuilds on every start), CDC reconciliation (rebuilds tables from topics), `bronze-replay`, and the customer-feature reference in the checks.

## Repair

1. Stop the deletion: `rpk cluster config set -- log_retention_ms -1` (topics on the default now keep everything).
2. Restore current state to the topics: a Debezium incremental snapshot of the six domain tables (`make cdc-snapshot`), triggered through the new signaling table `public.debezium_signal`. It re-emits each current row as an `r` event while streaming continues. The Flink feature job and the decision service read only creations (`op = 'c'`) and ignore them; bronze appends them; silver, SCD2 (no tracked change) and the offline model treat them as current state.
3. The checks read profile-change history from bronze, the complete record, instead of Kafka.

## Prevention

- `redpanda-config` one-shot sets unlimited retention on every `make up`, including after `make nuke`.
- `public.debezium_signal` stays: a future gap is repaired with `make cdc-snapshot`, without restarting anything.
- History questions are answered from bronze; Kafka is the transport, the lake is the record.

## Follow-up: a sink restart replayed all history

Verifying the repair, a feature-sink restart replayed about 1.4 million records (`features.card` plus the CDC topics, now including the snapshot) before reaching new ones. For that minute every decision was a `stale_features` review: fail-safe, never an approval, but a restart should not degrade decisions, and unlimited retention makes the replay grow forever. Once caught up, the same traffic decided normally (0.25% stale, p95 1.0 s).

- The sink now uses one stable consumer group and records in Redis which Redis instance (`run_id`) its state was built in. A sink restart resumes from the committed offsets; only an empty Redis triggers the full rebuild.
- It writes a readiness file once every partition has caught up, and its healthcheck requires it, so nothing waits on a cold store.
- `make bronze-reconcile` compares bronze with each topic from its first retained offset; older bronze rows are reported as history kept only in bronze, not as extra rows.

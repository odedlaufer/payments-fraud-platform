# decision_service

Online feature store and (Phase 3.4) real-time decisions.

| Entry point | Does |
| --- | --- |
| `feature-sink` | `features.card` (Kafka) -> Redis hash `card:{card_id}` per card (FR-F3) |

The sink writes through an atomic Lua script that keeps a record only if it is newer than the stored one, by `(event_ms, transaction_id)`. The feature job re-emits old records when it rebuilds its state, and delivery is at-least-once, so those writes are no-ops. Redis keeps no persistence: on every start the sink rebuilds the store from the beginning of `features.card`.

Each hash holds the card features plus `transaction_id` (the latest transaction they include), `event_ms`, `definitions_version`, `computed_at` (Flink) and `written_at_ms` (sink).

```bash
make online-check   # every card in Redis == the newest features.card record for that card
```

# simulator

Synthetic payments app that writes to the OLTP Postgres (PRD section 5.1). Everything is generated from a seed, so a run is reproducible.

```bash
make seed                    # reference data: customers, cards, merchants
make sim                     # real time: 20 tx/s, 1 simulated day = 10 real minutes
make sim SPEED=5 TPS=50      # faster clock, more traffic
uv run simulator run --fast --sim-days 1      # no pacing, deterministic for a seed
uv run simulator run --max-transactions 5000  # stop after N transactions
````

A run resumes after the latest simulated event in the database, so the timeline only moves forward. On stop (limit reached or Ctrl+C) it lets pending transactions resolve, then prints how many inserts, updates and deletes it made per table, which is what CDC must reproduce.

Transactions are inserted as `pending` and updated to `approved` or `declined` 1 to 10 simulated seconds later; about 1% of approvals are `reversed` hours to days later. `event_ts` on a transaction is its authorization time and never changes. Customers, cards and merchants change over time (profile edits, card blocks and expiries, limit changes, merchant updates and deletes), and those updates move `event_ts`, which drives SCD2.

| Module | Purpose |
| --- | --- |
| `clock.py` | Accelerated simulated clock (default 1 simulated day = 10 real minutes) |
| `geo.py` | Cities with coordinates, used for homes, merchants and travel distance |
| `catalog.py` | Merchant categories (MCC) and their typical amounts |
| `entities.py` | Seeded generators for customers, cards and merchants |
| `rates.py` | Event rates and lifecycle settings |
| `world.py` | In-memory view of the entities the engine acts on |
| `engine.py` | Event-driven simulation on simulated time |
| `store.py` | Writes: Postgres, or in memory for unit tests |
| `runner.py` | Real-time or fast loop, stop conditions, drain |
| `db.py` | Connection settings and seeding inserts |
| `cli.py` | `simulator` command line |

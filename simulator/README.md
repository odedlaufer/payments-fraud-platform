# simulator

Synthetic payments app that writes to the OLTP Postgres (PRD section 5.1). Everything is generated from a seed, so a run is reproducible.

```bash
make seed                                   # reference data: customers, cards, merchants
uv run simulator seed --customers 2000 --merchants 300 --seed 42
```

| Module | Purpose |
| --- | --- |
| `clock.py` | Accelerated simulated clock (default 1 simulated day = 10 real minutes) |
| `geo.py` | Cities with coordinates, used for homes, merchants and travel distance |
| `catalog.py` | Merchant categories (MCC) and their typical amounts |
| `entities.py` | Seeded generators for customers, cards and merchants |
| `db.py` | Inserts into Postgres |
| `cli.py` | `simulator` command line |

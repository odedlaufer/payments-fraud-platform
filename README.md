# Payments CDC Lakehouse + Real-Time Fraud Feature Platform

Local, self-hosted platform built entirely on free tools: a simulated payments app writes to Postgres, Debezium streams every change, Flink computes real-time fraud features and decisions, and an Iceberg lakehouse keeps full history for SCD2, point-in-time training sets, feature parity checks and rule backtesting.

> Status: Phase 1 complete: OLTP schema, Debezium CDC to Redpanda (Avro), simulator; CDC reconciles exactly against Postgres. Next: Phase 2, lakehouse bronze, silver and SCD2.

## Requirements

- Docker + Docker Compose, 16 GB RAM
- [uv](https://docs.astral.sh/uv/) and GNU Make

## Quick start

```bash
make setup   # dev deps, git hooks, .env
make check   # lint, typecheck, tests
make up      # start the stack and wait until healthy
make gate    # Trino writes and reads an Iceberg table end to end
make seed    # simulator reference data
make sim     # simulated payments traffic (20 tx/s)
make phase1-gate  # 10,000 transactions, then CDC must match Postgres exactly
make down    # stop, keep data
```

## Repository layout

| Path | Purpose |
| --- | --- |
| `simulator/` | Payments app simulator and fraud patterns |
| `postgres/` | Migrations, publication, heartbeat |
| `connect/` | Debezium connector configs |
| `schemas/` | Avro schemas |
| `flink/` | Feature jobs, Iceberg sinks, UDFs |
| `features/` | Single feature definition file |
| `decision_service/` | Real-time decision consumer |
| `rules/` | Versioned rules + shared rule engine |
| `backtester/` | Rule backtesting CLI |
| `dbt/` | Silver, SCD2, marts, training set, parity |
| `orchestration/` | Dagster |
| `contracts/` | Data contracts |
| `observability/` | Prometheus, Grafana, alerts |
| `chaos/` | Fault injection |
| `trino/` | Trino JVM and catalog config |
| `scripts/` | Health check, catalog bootstrap, gate |
| `docs/` | Architecture, ADRs, runbook |

## License

MIT

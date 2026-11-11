# Payments CDC Lakehouse + Real-Time Fraud Feature Platform

Local, fully open-source platform: a simulated payments app writes to Postgres, Debezium streams every change, Flink computes real-time fraud features and decisions, and an Iceberg lakehouse keeps full history for SCD2, point-in-time training sets, feature parity checks and rule backtesting.

> Status: Phase 0 — setup in progress.

## Requirements

- Docker + Docker Compose, 16 GB RAM
- [uv](https://docs.astral.sh/uv/) and GNU Make

## Quick start

```bash
make setup   # dev deps, git hooks, .env
make check   # lint, typecheck, tests
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
| `docs/` | Architecture, ADRs, runbook |

## License

MIT

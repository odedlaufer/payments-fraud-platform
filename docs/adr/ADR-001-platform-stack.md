# ADR-001: Platform stack

- **Status:** Accepted
- **Date:** 2026-11-11

## Context

The platform captures every change from a payments OLTP database, computes fraud features and decisions in seconds, and keeps full history for SCD2, point-in-time training sets, feature parity and rule backtesting (PRD section 1). It must run on one laptop with 16 GB RAM, use free tools only, and run a reduced stack in CI. Lakehouse storage, catalog and versions are decided separately in ADR-002.

## Decisions

| Layer | Chosen | Alternatives considered | Why |
| --- | --- | --- | --- |
| Event streaming | Redpanda | Apache Kafka (KRaft) + separate schema registry | Single binary with a built-in Schema Registry and HTTP proxy, and a much smaller memory footprint. It speaks the Kafka API, so Debezium, Flink and Python clients are unchanged. |
| CDC | Debezium Postgres connector on Kafka Connect | Flink CDC, Debezium Server | Industry-standard CDC with Avro and Schema Registry converters and offsets stored in Kafka. Changes land in the log first, so the hot path, the cold path and any replay read the same events. Flink CDC would tie capture to the stream job and skip the log. |
| Stream processing | Apache Flink (Flink SQL, PyFlink for UDFs) | Spark Structured Streaming, Kafka Streams, RisingWave | Per-transaction features need row-level trailing windows (`OVER ... RANGE INTERVAL`) on event time with watermarks and keyed state. Flink does this with exactly-once checkpoints and sub-second latency. Spark is micro-batch and lacks streaming OVER windows; Kafka Streams is JVM code only; RisingWave is a narrower skill set. |
| Online feature store | Redis | Feast, a database table | Sub-millisecond lookups by entity id with no extra service. Feast is noted as the at-scale option (PRD 10.4). |
| Decision service | Python (FastAPI + confluent-kafka) | Flink job, JVM service | The rule engine is a Python package imported by both the decision service and the backtester (FR-D2), so production and backtests run the same code. |
| Table format | Apache Iceberg | Delta Lake, Apache Hudi | Engine-neutral REST catalog and first-class support in Flink, Trino and DuckDB; snapshots give time travel for reproducible training sets and backtests (FR-PIT4, FR-BT1). Details in ADR-002. |
| Batch SQL and dbt target | Trino | Apache Spark | Runs as one small coordinator, supports Iceberg `MERGE`, branches and metadata tables, and has a maintained dbt adapter. Spark would need a driver and executors on the same laptop. |
| Local and backtest SQL | DuckDB | Trino for everything | In-process, no server, fast on a laptop and in CI; reads Iceberg directly. |
| Transformations | dbt Core | Hand-written SQL jobs | Tested, documented models with contracts and snapshots (SCD2), runnable on Trino and DuckDB. |
| Orchestration | Dagster | Apache Airflow, Prefect | Software-defined assets with lineage, native dbt integration, and asset checks that block downstream work, which maps directly to the DQ gates (FR-O4). |
| Data quality | dbt tests, datacontract-cli, one non-dbt tool | — | Choice of Great Expectations or Soda Core deferred to ADR-004 (PRD Q4). |
| Observability | Prometheus, Grafana, OpenLineage + Marquez | Hosted APM | Free, self-hosted metrics, dashboards, alerts and dataset lineage. |

## Licensing note

Redpanda Community Edition is published under the Business Source License 1.1, which is free for this use but is not an OSI-approved open-source licence. Everything else in the stack is Apache-2.0, MIT or similar. If a strictly open-source stack is required, Redpanda is replaced by Apache Kafka in KRaft mode plus an Apache-licensed schema registry; no client, connector or job changes, because they all use the Kafka API.

## Consequences

- The JVM services (Kafka Connect, Flink, Trino) dominate memory. Compose profiles (`core`, `lake`, later `stream` and `obs`) let a developer run only what a task needs.
- Application code is Python 3.12; pipelines are SQL (Flink SQL, dbt).
- Every image and artifact follows the pinning policy in ADR-002: exact versions, at least 14 days old.

# Decisions log

The PRD's definition of done: every FR, DQ and DC requirement is built, or explicitly deferred
here. Everything not listed below is built and tested. Status as of 2026-11-24.

## Requirements not built as written

| Requirement | What exists instead | Decision |
| --- | --- | --- |
| FR-L4: dbt model contracts on every mart; source freshness checks | ODCS data contracts on the published datasets, tested live against Trino and against `main` in CI (DC-1 to DC-3); the LakehouseStale alert on every bronze table | Deferred. Enforced dbt contracts declare every column's type in every mart; worth it once others consume the marts |
| DQ-05: feature values in valid ranges, else review | Features come from one definition file (ADR-003); parity compares every decision's features with an offline recomputation | Deferred. A range check in the decision context, sending review and a metric |
| DQ-15: a backtest refuses unmatured labels | Met by construction: published training sets contain only transactions old enough for their label to have matured | No separate check needed |
| OBS-3: `ops.pipeline_runs`, `ops.contract_checks` | Dagster keeps every run's history; contract results are in CI and `make contracts-check`. Six other `ops` tables exist (alerts, dq_results, maintenance_runs, parity_mismatches, rule_promotions, training_sets) | Deferred |
| OBS-4: structured JSON logs, `transaction_id` as correlation id | Decisions carry `transaction_id` in the topic and in bronze; dead letters carry their source topic, partition and offset | Deferred |
| ENG-3: images pinned by tag and digest | Every image pinned to an exact version tag | Deferred: digests next |
| Coverage of at least 80% | 75% unit-test coverage, measured 2026-11-24: rule engine 98%, simulator engine 97%, decision logic 100%; the Kafka, Redis and Postgres loops (27 to 52%) are tested live by the integration tests, the failure tests and the reconciles | Deferred: count integration-test coverage too |
| Detection of merchant bust-out | No rule yet | A known gap, stated in the README |
| A demo GIF | Screenshots in the README; the demo console's auto-play is the live demo | Replaced |

## Open questions, resolved

| Question | Answer |
| --- | --- |
| Q1: Flink Iceberg sink or Iceberg Kafka Connect sink | The Flink Iceberg sink, exactly once ([ADR-002](adr/ADR-002-lakehouse-storage-catalog-sink.md)) |
| Q2: Iceberg REST catalog, Nessie or Lakekeeper | Lakekeeper, a REST catalog ([ADR-002](adr/ADR-002-lakehouse-storage-catalog-sink.md)) |
| Q3: dbt snapshots, or a custom SCD2 | Custom: history rebuilt from the CDC log itself ([ADR-006](adr/ADR-006-scd2-from-cdc-log.md)) |
| Q4: Great Expectations or Soda Core for checks outside dbt | Neither: dbt tests, Dagster asset checks, reconcile scripts and ODCS contracts with datacontract-cli cover every gate |
| Q5: Elementary on dbt-trino | Not used: Grafana and the `ops` tables |
| Q6: simulator throughput and clock speed for 16 GB | 20 payments a second at 10 minutes per simulated day; the stack's memory limits are tested to fit a 12 GB Docker VM |

## Decisions without their own ADR

- **ADR-004, a quality tool outside dbt:** none (Q4 above).
- **ADR-005, stale features mean review, never approve:** built as FR-D4. Failure test CH-3
  held all 2,350 decisions made while Redis was down for review, with 0 approvals.

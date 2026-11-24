# chaos

Fault injection CH-1 to CH-6 (PRD section 8.5): `make chaos` runs each scenario during
traffic, measures recovery and checks the expected outcome with the project's own checks
(bronze, CDC and silver reconciliation, the online store, decisions, Prometheus metrics).
Results: `chaos/results.json`, rendered to `docs/chaos.md`. Stop Dagster first.

`ch1-bronze.sh` is the original Phase 2 bronze-only version of CH-1 (`make chaos-ch1-bronze`).

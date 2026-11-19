# backtester

Replays any rules version over a published training set (PRD 5.8, FR-BT1, FR-BT2).

```bash
make backtest RULES=rules/rules_v2.yml                       # latest published training set
uv run backtest --rules rules/rules_v1.yml --snapshot-id 8254010497186583662
uv run backtest --rules rules/rules_v2.yml --from 2026-01-10 --to 2026-01-20
```

- **Input:** a training set published by `make publish-training-set`, read through DuckDB's
  Iceberg extension from the lake **at its Iceberg snapshot** (Lakekeeper REST catalog,
  SeaweedFS S3). Labels are matured and as known at the set's cutoff (DQ-15 holds by
  construction).
- **Rules:** the decision service's own engine (`rules`) on its own context
  (`decision_service.context`): the features plus amount, channel and device risk.
- **Metrics,** overall and per rule: alerts, precision, recall, alert rate, $ fraud declined,
  $ legit declined. An alert is a review or a decline.
- **Output:** a Markdown report in `backtester/reports/` and rows in
  `lake.marts.rule_backtests`, each with the rules file's SHA-256, the snapshot id and the
  cutoff. Same rules file and same snapshot give the same metrics.

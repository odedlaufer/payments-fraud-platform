# rules

Versioned fraud rules (`rules_vN.yml`) and the engine that evaluates them. The decision service and the backtester import the same package, so a rule behaves identically online and in a backtest (FR-D2).

```yaml
version: 1
description: ...
rules:
  - id: card_testing                       # lower snake_case, unique
    description: ...
    condition: txn_count_1h >= 5 and distinct_merchants_1h >= 4
    action: decline                        # review | decline
    severity: 90                           # 1-100; the decision score is the highest fired
```

**Conditions** use comparisons (`> >= < <= == !=`), `and`, `or`, `not`, `+ - * /`, numbers, strings and the names a context provides (card features, `amount`, `channel`, `device_risk_score`). They are parsed against a whitelist and compiled; nothing is passed to `eval`. Unknown names, calls, attributes and subscripts are rejected when the file loads.

**Nulls** never make a rule fire: arithmetic with a null is null, and a comparison with a null is false.

**Outcome**: the most severe action among fired rules (decline > review), else approve.

A rules file is immutable once it has decided live traffic: changes go into a new version, which is backtested against the current one before promotion (Phase 5).

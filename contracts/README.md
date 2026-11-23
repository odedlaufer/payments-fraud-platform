# contracts

Data contracts for the published datasets (PRD 6.1), in the Open Data Contract Standard
(ODCS v3.2), validated with datacontract-cli 1.2.9.

| Contract | Dataset | Schema from |
| --- | --- | --- |
| `decisions.odcs.yaml` | the `decisions` topic | the decision service's Avro schema |
| `silver.odcs.yaml` | the six `lake.silver.*_current` tables | Trino |
| `training_set.odcs.yaml` | `lake.ml.training_set` | Trino |

- **`agreements.yaml`** is the human part: owner, purpose, SLOs (PRD section 2) and quality
  rules. Edit it, then `make contracts` regenerates the contracts from the live schemas;
  regeneration never changes a contract's version.
- **`make contracts-check`** (live): the contracts still match the schemas, `datacontract test`
  runs the quality rules against Trino, and the schema registry enforces `BACKWARD` (DC-1).
- **`make check`** (offline): every contract is valid ODCS; the decisions contract equals the
  service's Avro schema; and a change `datacontract breaking` flags against `main` needs a new
  major version (DC-3).

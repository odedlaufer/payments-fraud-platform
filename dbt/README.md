# dbt

Transformations on Trino over the Iceberg lakehouse (PRD 5.5).

| Layer | Models | Built from |
| --- | --- | --- |
| staging | `stg_cdc__{table}`: one typed row per change event (views) | `lake.bronze.cdc_{table}` |
| silver | `{table}_current`: one row per key, latest committed state (incremental MERGE) | staging |

Staging models and the bronze sources are generated from the Postgres schema by `make gen-bronze`; do not edit them by hand.

**Ordering.** Silver keeps, per key, the event with the highest Kafka offset. Debezium emits changes in commit order, so that is the latest committed state. LSN is kept on every row but not used for ordering: with concurrent transactions, a change committed later can have a smaller LSN.

**Deletes** stay as rows with `is_deleted = true` and the values they had before the delete.

```bash
make dbt-build      # run models and tests
make dbt-run        # models only
make dbt-test       # tests only
```

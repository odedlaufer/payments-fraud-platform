# dbt

Transformations on Trino over the Iceberg lakehouse (PRD 5.5).

| Layer | Models | Built from |
| --- | --- | --- |
| staging | `stg_cdc__{table}`: one typed row per change event (views) | `lake.bronze.cdc_{table}` |
| silver | `{table}_current`: one row per key, latest committed state (incremental MERGE) | staging |
| snapshots | `{customers,cards,merchants}_scd2`: one row per version, `valid_from` / `valid_to` / `is_current` | staging (ADR-006) |

Staging models and the bronze sources are generated from the Postgres schema by `make gen-bronze`; do not edit them by hand.

**Ordering.** Silver keeps, per key, the event with the highest Kafka offset. Debezium emits changes in commit order, so that is the latest committed state. LSN is kept on every row but not used for ordering: with concurrent transactions, a change committed later can have a smaller LSN.

**Deletes** stay as rows with `is_deleted = true` and the values they had before the delete.

**SCD2** is rebuilt from the full change log, not dbt snapshots, so no version between runs is lost (ADR-006). Tests check one current version per key, contiguous ranges with no gaps or overlaps, and that each current version equals silver.

```bash
make dbt-build      # run models and tests
make dbt-run        # models only
make dbt-test       # tests only
```

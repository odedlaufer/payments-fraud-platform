{% docs __overview__ %}
# Payments fraud platform: data models

These models turn the change log of a payments database into history, features and training
sets. Every model below is built and tested by dbt on Trino over an Apache Iceberg lakehouse.

**How the layers fit together**

- **Sources** (`bronze`): every insert, update and delete captured from Postgres by Debezium,
  landed append-only by Flink, plus every fraud decision with the features it used.
- **Staging and silver**: one row per key with its latest state, applied from the change log
  in log order; deletes kept as `is_deleted`. Silver equals Postgres row for row (checked on
  every build).
- **Snapshots**: full history of customers, cards and merchants (SCD2) rebuilt from the change
  log itself, so every past state is exact.
- **Marts**: transactions joined to the customer, card and merchant *as they were* at payment
  time; labels with the time they became known; features recomputed offline.
- **Training sets**: point-in-time sets per cutoff. Only transactions old enough for their
  label to have matured, only labels known by the cutoff, features as of each payment.
- **Parity**: the features each decision used, compared with the offline recomputation.

Open the lineage graph (bottom right) to follow a payment from the raw change event to the
training set. Project: [github.com/odedlaufer/payments-fraud-platform](https://github.com/odedlaufer/payments-fraud-platform).
{% enddocs %}

-- DQ-12: a label cannot be known before the transaction happened. Blocks training sets.
select
    transaction_id,
    event_ts,
    label_known_at
from {{ ref('fct_labels') }}
where label_known_at < event_ts

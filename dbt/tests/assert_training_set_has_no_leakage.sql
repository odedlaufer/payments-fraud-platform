-- FR-PIT3 / DQ-13: nothing in the training set may be known after its boundary. Any row
-- returned fails the build (and blocks publishing):
--   the transaction is not mature at the cutoff; a positive label became known after the
--   cutoff (or before the transaction); a dimension version started after the event; the
--   features were computed for another instant than the event; the cutoff is later than the
--   simulation's current time (labels that will exist by then have not happened yet).
{% set maturity = var("label_maturity_days", 60) %}
with simulated as (
    select max(l.as_of_ts) as now_ts from {{ ref('fct_labels') }} as l
)

select
    t.transaction_id,
    t.event_ts,
    t.cutoff,
    t.label,
    t.label_known_at
from {{ ref('training_set') }} as t
cross join simulated as k
where
    t.event_ts > t.cutoff - interval '{{ maturity }}' day
    or (t.label = 1 and (t.label_known_at is null or t.label_known_at > t.cutoff))
    or t.label_known_at < t.event_ts
    or t.card_version_from > t.event_ts
    or t.customer_version_from > t.event_ts
    or t.merchant_version_from > t.event_ts
    or t.event_ms != {{ epoch_ms('t.event_ts') }}
    or t.cutoff > k.now_ts

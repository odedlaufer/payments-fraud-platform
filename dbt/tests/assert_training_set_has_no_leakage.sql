-- FR-PIT3 / DQ-13: nothing in the training set may be known after its boundary. Any row
-- returned fails the build (and blocks publishing):
--   the transaction is not mature at the cutoff; a positive label became known after the
--   cutoff (or before the transaction); a dimension version started after the event; the
--   features were computed for another instant than the event; the cutoff is later than the
--   simulation's current time (labels that will exist by then have not happened yet); the
--   transaction predates label coverage (it could never have been labelled).
{% set maturity = var("label_maturity_days", 60) %}
with simulated as (
    select max(l.as_of_ts) as now_ts from {{ ref('fct_labels') }} as l
),

coverage as (
    -- computed here, not taken from the model: the first transaction whose label was known
    -- at the training set's cutoff
    select min(l.event_ts) as label_coverage_from
    from {{ ref('fct_labels') }} as l
    where
        l.label = 1
        and l.label_known_at <= (select max(x.cutoff) from {{ ref('training_set') }} as x)
)

select
    t.transaction_id,
    t.event_ts,
    t.cutoff,
    t.label,
    t.label_known_at
from {{ ref('training_set') }} as t
cross join simulated as k
cross join coverage as c
where
    t.event_ts > t.cutoff - interval '{{ maturity }}' day
    or (t.label = 1 and (t.label_known_at is null or t.label_known_at > t.cutoff))
    or t.label_known_at < t.event_ts
    or t.card_version_from > t.event_ts
    or t.customer_version_from > t.event_ts
    or t.merchant_version_from > t.event_ts
    or t.event_ms != {{ epoch_ms('t.event_ts') }}
    or t.cutoff > k.now_ts
    or t.event_ts < c.label_coverage_from

-- Point-in-time training set (FR-PIT2): for a cutoff, the transactions mature by then
-- (event_ts <= cutoff - label_maturity_days), their features as of the event (offline
-- features, equal to the reference), dimensions as of the event, and the label as it was
-- known at the cutoff: a chargeback filed after the cutoff does not exist for this set.
--   dbt build --select training_set+ --vars '{training_cutoff: "2026-03-30 00:00:00+00:00"}'
-- Default cutoff: the simulation's current time. Never selects the current transaction
-- status: a later reversal would leak the future.
{% set cutoff = var("training_cutoff", none) %}
{% set maturity = var("label_maturity_days", 60) %}

with clock as (
    -- exactly one row: the given cutoff, or the simulation's current time
    {% if cutoff %}
        select cast('{{ cutoff }}' as timestamp (6) with time zone) as cutoff
    {% else %}
        select max(as_of_ts) as cutoff from {{ ref('fct_labels') }}
    {% endif %}
),

eligible as (
    select
        l.transaction_id,
        l.event_ts,
        l.label,
        l.label_known_at,
        k.cutoff
    from {{ ref('fct_labels') }} as l
    cross join clock as k
    where l.event_ts <= k.cutoff - interval '{{ maturity }}' day
)

select
    f.*,
    e.event_ts,
    e.cutoff,
    x.amount,
    x.currency,
    x.channel,
    x.device_risk_score,
    x.card_type,
    x.card_status_at_event,
    x.card_credit_limit_at_event,
    x.customer_risk_tier_at_event,
    x.customer_country,
    x.merchant_mcc_at_event,
    x.merchant_category_at_event,
    x.merchant_country,
    x.merchant_status_at_event,
    x.card_version_from,
    x.customer_version_from,
    x.merchant_version_from,
    case when e.label = 1 and e.label_known_at <= e.cutoff then 1 else 0 end as label,
    case when e.label = 1 and e.label_known_at <= e.cutoff then e.label_known_at end
        as label_known_at
from eligible as e
inner join {{ ref('card_offline') }} as f on e.transaction_id = f.transaction_id
inner join {{ ref('fct_transactions') }} as x on e.transaction_id = x.transaction_id

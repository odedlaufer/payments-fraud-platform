-- Independent check of the as-of join (FR-L3): for every transaction, the card status,
-- customer risk tier and merchant MCC it got must equal those of the latest version that
-- started at or before its event time, found with max_by and without valid_to at all.
with t as (
    select
        transaction_id,
        card_id,
        customer_id,
        merchant_id,
        event_ts,
        card_status_at_event,
        customer_risk_tier_at_event,
        merchant_mcc_at_event
    from {{ ref('fct_transactions') }}
),

card as (
    select
        t.transaction_id,
        max_by(c.status, c.valid_from) as status
    from t inner join {{ ref('cards_scd2') }} as c
        on t.card_id = c.card_id and t.event_ts >= c.valid_from
    group by t.transaction_id
),

customer as (
    select
        t.transaction_id,
        max_by(u.risk_tier, u.valid_from) as risk_tier
    from t inner join {{ ref('customers_scd2') }} as u
        on t.customer_id = u.customer_id and t.event_ts >= u.valid_from
    group by t.transaction_id
),

merchant as (
    select
        t.transaction_id,
        max_by(m.mcc, m.valid_from) as mcc
    from t inner join {{ ref('merchants_scd2') }} as m
        on t.merchant_id = m.merchant_id and t.event_ts >= m.valid_from
    group by t.transaction_id
)

select t.transaction_id
from t
left join card as c on t.transaction_id = c.transaction_id
left join customer as u on t.transaction_id = u.transaction_id
left join merchant as m on t.transaction_id = m.transaction_id
where
    t.card_status_at_event is distinct from c.status
    or t.customer_risk_tier_at_event is distinct from u.risk_tier
    or t.merchant_mcc_at_event is distinct from m.mcc

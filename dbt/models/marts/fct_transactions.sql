-- One row per transaction, joined with card, customer and merchant attributes as they were
-- at the transaction's event time (FR-L3), not as they are now. A version is valid on
-- [valid_from, valid_to); the card version at event time names the customer.
with transactions as (
    select *
    from {{ ref('transactions_current') }}
    where not is_deleted
)

select
    t.transaction_id,
    t.card_id,
    cv.customer_id,
    t.merchant_id,
    t.session_id,
    t.event_ts,
    t.status_event_ts,
    t.status,
    t.amount,
    t.currency,
    t.channel,
    t.lat,
    t.lon,
    t.device_risk_score,
    cv.card_type,
    cv.status as card_status_at_event,
    cv.credit_limit as card_credit_limit_at_event,
    cv.expires_on as card_expires_on,
    uv.risk_tier as customer_risk_tier_at_event,
    uv.country_code as customer_country,
    uv.home_lat as customer_home_lat,
    uv.home_lon as customer_home_lon,
    mv.mcc as merchant_mcc_at_event,
    mv.category as merchant_category_at_event,
    mv.country_code as merchant_country,
    mv.status as merchant_status_at_event,
    cv.valid_from as card_version_from,
    uv.valid_from as customer_version_from,
    mv.valid_from as merchant_version_from
from transactions as t
left join {{ ref('cards_scd2') }} as cv
    on
        t.card_id = cv.card_id
        and cv.valid_from <= t.event_ts
        and (cv.valid_to is null or t.event_ts < cv.valid_to)
left join {{ ref('customers_scd2') }} as uv
    on
        cv.customer_id = uv.customer_id
        and uv.valid_from <= t.event_ts
        and (uv.valid_to is null or t.event_ts < uv.valid_to)
left join {{ ref('merchants_scd2') }} as mv
    on
        t.merchant_id = mv.merchant_id
        and mv.valid_from <= t.event_ts
        and (mv.valid_to is null or t.event_ts < mv.valid_to)

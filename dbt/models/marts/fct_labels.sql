-- One row per transaction: its chargeback label and when the label became known (FR-PIT1).
-- Times are simulated (ADR-008): label_known_at is the chargeback's filing time, on the same
-- clock as event_ts, feature windows and training cutoffs. A transaction is mature once
-- label_maturity_days have passed, enough time for any chargeback to arrive (FR-SIM4).
with transactions as (
    select
        transaction_id,
        card_id,
        merchant_id,
        event_ts
    from {{ ref('transactions_current') }}
    where not is_deleted
),

chargebacks as (
    select
        transaction_id,
        chargeback_id,
        reason_code,
        event_ts as filed_at
    from {{ ref('chargebacks_current') }}
    where not is_deleted
),

clock as (
    -- The simulation's current time: the latest transaction it has produced.
    select max(event_ts) as now_ts from transactions
)

select
    t.transaction_id,
    t.card_id,
    t.merchant_id,
    t.event_ts,
    cast(c.chargeback_id is not null as integer) as label,
    c.filed_at as label_known_at,
    c.reason_code,
    t.event_ts + interval '{{ var("label_maturity_days", 60) }}' day as matures_at,
    t.event_ts + interval '{{ var("label_maturity_days", 60) }}' day <= k.now_ts as is_mature,
    k.now_ts as as_of_ts
from transactions as t
left join chargebacks as c on t.transaction_id = c.transaction_id
cross join clock as k

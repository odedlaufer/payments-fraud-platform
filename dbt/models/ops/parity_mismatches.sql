-- Every parity mismatch with both values and a likely cause (FR-PAR3):
--   definition_drift  the decision logged an older definitions_version;
--   late_event        a transaction on the same card or merchant arrived after this one but
--                     is earlier in event time (within the widest window): Flink's watermark
--                     drops such events, the lake counts them;
--   unexplained       neither: investigate.
with mismatches as (
    select * from {{ ref('parity_comparisons') }}
    where compared and not matches
),

arrivals as (
    select
        transaction_id,
        min(_kafka_offset) as arrival
    from {{ ref('stg_cdc__transactions') }}
    where _op in ('c', 'r')
    group by transaction_id
),

timeline as (
    select
        o.transaction_id,
        o.card_id,
        o.merchant_id,
        o.event_ms,
        a.arrival
    from {{ ref('card_offline') }} as o
    inner join arrivals as a on o.transaction_id = a.transaction_id
),

current_version as (
    select max(definitions_version) as current_definitions from {{ ref('card_offline') }}
),

late as (
    -- mismatched transactions with an earlier-in-time event that arrived after them
    select distinct m.transaction_id
    from mismatches as m
    inner join timeline as me on m.transaction_id = me.transaction_id
    inner join timeline as o
        on (me.card_id = o.card_id or me.merchant_id = o.merchant_id)
    where
        o.arrival > me.arrival
        and o.event_ms <= me.event_ms
        and o.event_ms >= me.event_ms - {{ var("parity_late_window_ms", 2592000000) }}
)

select
    m.transaction_id,
    m.event_day,
    m.feature,
    m.entity,
    m.logged_value,
    m.offline_value,
    m.abs_diff,
    m.decision,
    m.logged_version,
    m.decided_at,
    case
        when m.logged_version != v.current_definitions then 'definition_drift'
        when l.transaction_id is not null then 'late_event'
        else 'unexplained'
    end as cause
from mismatches as m
cross join current_version as v
left join late as l on m.transaction_id = l.transaction_id

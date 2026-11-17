-- label = 1 exactly when a chargeback exists, and then label_known_at is its filing time.
select
    l.transaction_id,
    l.label,
    l.label_known_at,
    c.event_ts as filed_at
from {{ ref('fct_labels') }} as l
left join {{ ref('chargebacks_current') }} as c
    on l.transaction_id = c.transaction_id and not c.is_deleted
where
    (l.label = 1) != (c.chargeback_id is not null)
    or l.label_known_at is distinct from c.event_ts

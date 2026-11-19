-- Every label equals what was knowable at the cutoff: 1 exactly when a chargeback was filed
-- by then. Catches both leakage (a later chargeback counted) and lost labels.
select
    t.transaction_id,
    t.label,
    l.label as label_now,
    l.label_known_at,
    t.cutoff
from {{ ref('training_set') }} as t
inner join {{ ref('fct_labels') }} as l on t.transaction_id = l.transaction_id
where t.label != case when l.label = 1 and l.label_known_at <= t.cutoff then 1 else 0 end

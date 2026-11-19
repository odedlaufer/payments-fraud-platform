-- Online/offline parity per simulated day (FR-PAR2): per feature, and per decision, the
-- PRD's SLO: the share of decisions whose every logged feature equals the offline value.
-- Rows not compared (stale reviews; groups logged before they existed) are counted apart.
with comparisons as (
    select * from {{ ref('parity_comparisons') }}
),

per_feature as (
    select
        event_day,
        feature,
        count_if(compared) as compared,
        count_if(compared and matches) as matching,
        count_if(not compared) as not_compared,
        max(case when compared then abs_diff end) as max_abs_diff
    from comparisons
    group by event_day, feature
),

per_decision as (
    select
        event_day,
        transaction_id,
        bool_or(compared) as any_compared,
        bool_and(matches or not compared) as all_match
    from comparisons
    group by event_day, transaction_id
),

all_features as (
    select
        event_day,
        '(all features)' as feature,
        cast(null as double) as max_abs_diff,
        count_if(any_compared) as compared,
        count_if(any_compared and all_match) as matching,
        count_if(not any_compared) as not_compared
    from per_decision
    group by event_day
)

select
    event_day,
    feature,
    compared,
    matching,
    not_compared,
    cast(matching as double) / nullif(compared, 0) as match_rate,
    max_abs_diff
from per_feature
union all
select
    event_day,
    feature,
    compared,
    matching,
    not_compared,
    cast(matching as double) / nullif(compared, 0) as match_rate,
    max_abs_diff
from all_features

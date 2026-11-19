-- DQ-14: online/offline parity >= 99.9% of compared decisions (FR-PAR2, PRD section 2).
-- Fails the build (and so blocks rule promotion) when parity falls below the SLO.
with overall as (
    select
        sum(compared) as compared,
        sum(matching) as matching
    from {{ ref('parity_daily') }}
    where feature = '(all features)'
)

select
    compared,
    matching,
    cast(matching as double) / compared as match_rate
from overall
where compared > 0 and cast(matching as double) / compared < {{ var("parity_slo", 0.999) }}

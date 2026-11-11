-- The current SCD2 version of every live key equals silver's current state, and
-- deleted keys have no current version: two independent builds of the same history.
{% test scd2_current_matches_silver(model, pk, silver) %}
{% set tracked = scd2_tracked_columns(ref('stg_cdc__' ~ silver.replace('_current', '')), pk) %}
{% set cols = [pk] + tracked %}
with scd2_current as (
    select {{ cols | join(', ') }} from {{ model }} where is_current
),

silver_live as (
    select {{ cols | join(', ') }} from {{ ref(silver) }} where not is_deleted
),

only_in_scd2 as (
    select 'only in scd2' as side, * from (select * from scd2_current except select * from silver_live)
),

only_in_silver as (
    select 'only in silver' as side, * from (select * from silver_live except select * from scd2_current)
)

select * from only_in_scd2
union all
select * from only_in_silver
{% endtest %}

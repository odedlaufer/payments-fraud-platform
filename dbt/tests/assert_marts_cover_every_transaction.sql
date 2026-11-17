-- Both marts have exactly one row per transaction in silver.
with counts as (
    select
        (
            select count(*) from {{ ref('transactions_current') }}
            where not is_deleted
        ) as silver,
        (select count(*) from {{ ref('fct_transactions') }}) as transactions,
        (select count(*) from {{ ref('fct_labels') }}) as labels
)

select * from counts
where transactions != silver or labels != silver

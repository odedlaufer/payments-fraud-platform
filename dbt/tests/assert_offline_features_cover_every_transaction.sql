-- features.card_offline has exactly one row per transaction in silver.
with counts as (
    select
        (
            select count(*) from {{ ref('transactions_current') }}
            where not is_deleted
        ) as silver,
        (select count(*) from {{ ref('card_offline') }}) as offline
)

select * from counts
where offline != silver

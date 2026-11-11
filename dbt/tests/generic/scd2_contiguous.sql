-- DQ-10: every version has positive length, and versions of a key follow each other
-- with no gap and no overlap; only the last version may be open (valid_to null).
{% test scd2_contiguous(model, pk) %}
with ordered as (
    select
        {{ pk }},
        valid_from,
        valid_to,
        lead(valid_from) over (partition by {{ pk }} order by valid_from) as next_from
    from {{ model }}
)

select *
from ordered
where
    (valid_to is not null and valid_to <= valid_from)
    or (next_from is not null and valid_to is null)
    or (next_from is not null and valid_to <> next_from)
{% endtest %}

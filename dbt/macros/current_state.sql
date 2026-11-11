{#
  Current state of one table from its staged CDC events (FR-L1).

  Order is the Kafka offset: Debezium emits changes in commit order, so the
  highest offset per key is the latest committed state. LSN is not used for
  ordering because concurrent transactions can commit a smaller change LSN
  after a larger one; it is kept on every row for traceability.

  Incremental runs read only events past the highest offset already merged.
  `select distinct` collapses any duplicate bronze rows for the same offset.
#}
{% macro current_state(staged, pk) %}
with changes as (
    select *
    from {{ staged }}
    {% if is_incremental() %}
    where _kafka_offset > (
        select coalesce(max(_kafka_offset), -1) from {{ this }}
    )
    {% endif %}
),

latest as (
    select {{ pk }}, max(_kafka_offset) as _kafka_offset
    from changes
    group by {{ pk }}
)

select distinct
    c.*,
    -- Iceberg stores timestamps at microsecond precision; current_timestamp is millisecond.
    cast(current_timestamp as timestamp(6) with time zone) as _loaded_at
from changes as c
inner join latest as l
    on c.{{ pk }} = l.{{ pk }} and c._kafka_offset = l._kafka_offset
{% endmacro %}

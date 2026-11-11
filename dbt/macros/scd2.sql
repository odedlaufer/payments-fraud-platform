{#
  SCD2 history of one table, rebuilt from its full CDC change log (FR-L2, ADR-006).

  A version starts at the first event of a key and whenever a tracked column
  changes. valid_from is the change's simulated event_ts; valid_to is the next
  version's valid_from, or the delete time for a deleted key, or null while
  current. Tracked columns are every staged column except the key, timestamps
  and CDC metadata, discovered at run time so new columns are tracked too.

  Events are sequenced by Kafka offset (commit order, see current_state).
#}
{% macro scd2_untracked(pk) %}
    {{ return([pk, 'event_ts', 'created_at', 'updated_at', 'is_deleted',
               '_op', '_commit_ts', '_lsn', '_kafka_partition', '_kafka_offset']) }}
{% endmacro %}

{% macro scd2_tracked_columns(relation, pk) %}
    {% set untracked = scd2_untracked(pk) %}
    {% set tracked = [] %}
    {% if execute %}
        {% for col in adapter.get_columns_in_relation(relation) %}
            {% if col.name | lower not in untracked %}
                {% do tracked.append(col.name | lower) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(tracked) }}
{% endmacro %}

{% macro scd2(staged, pk) %}
{% set tracked = scd2_tracked_columns(staged, pk) %}
with events as (
    select
        *,
        to_hex(md5(to_utf8(concat_ws('|',
            {% for c in tracked %}coalesce(cast({{ c }} as varchar), '<null>'){{ "," if not loop.last }}
            {% endfor %}
        )))) as _version_hash
    from {{ staged }}
),

sequenced as (
    select
        *,
        lag(_version_hash) over (partition by {{ pk }} order by _kafka_offset) as _prev_hash
    from events
),

-- A version starts at the first event of a key, then whenever a tracked column changes.
starts as (
    select *
    from sequenced
    where not is_deleted and (_prev_hash is null or _prev_hash <> _version_hash)
),

-- Several changes at the same simulated instant collapse into the last one.
ranked as (
    select
        *,
        row_number() over (partition by {{ pk }}, event_ts order by _kafka_offset desc) as _rn
    from starts
),

versions as (
    select
        *,
        lead(event_ts) over (partition by {{ pk }} order by event_ts) as _next_from
    from ranked
    where _rn = 1
),

deletes as (
    select {{ pk }}, max(event_ts) as deleted_at
    from events
    where is_deleted
    group by {{ pk }}
)

select
    v.{{ pk }},
    {% for c in tracked %}v.{{ c }},
    {% endfor %}
    v.event_ts as valid_from,
    coalesce(v._next_from, d.deleted_at) as valid_to,
    v._next_from is null and d.deleted_at is null as is_current,
    v._version_hash,
    v._kafka_offset as _source_offset
from versions as v
left join deletes as d on v.{{ pk }} = d.{{ pk }}
{% endmacro %}

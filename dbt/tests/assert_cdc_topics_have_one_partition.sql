-- current_state() uses the highest merged Kafka offset as its watermark, which is
-- only correct while every CDC topic has a single partition. Fail loudly otherwise.
{% set tables = ['customers', 'cards', 'merchants', 'sessions', 'transactions', 'chargebacks'] %}
{% for t in tables %}
    select
        '{{ t }}' as table_name,
        _kafka_partition
    from {{ ref('stg_cdc__' ~ t) }}
    where _kafka_partition <> 0
    {% if not loop.last %}union all{% endif %}
{% endfor %}

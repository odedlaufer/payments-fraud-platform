{# Epoch milliseconds of a timestamp, truncated, in exact integer arithmetic (ADR-003). #}
{% macro epoch_ms(ts) -%}
    (cast(to_unixtime(date_trunc('second', {{ ts }})) as bigint) * 1000 + millisecond({{ ts }}))
{%- endmacro %}

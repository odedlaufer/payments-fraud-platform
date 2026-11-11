-- DQ-10: no key has more than one current version.
{% test scd2_one_current_version(model, pk) %}
select {{ pk }}, count(*) as current_versions
from {{ model }}
where is_current
group by {{ pk }}
having count(*) > 1
{% endtest %}

{#- Fails for rows outside min_value..max_value (inclusive). Nulls are left to not_null. -#}
{% test is_between(model, column_name, min_value, max_value) %}

select *
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}

{% endtest %}

{#- Fails for rows where the column is below zero. Nulls are left to not_null. -#}
{% test is_non_negative(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}

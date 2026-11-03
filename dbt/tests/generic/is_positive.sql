{#- Fails for rows where the column is zero or negative. Nulls are left to not_null. -#}
{% test is_positive(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}

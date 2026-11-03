{#-
    Business date in Japan time (JST).

    Every timestamp in the raw data is UTC, but sales are reported per Japanese calendar day.
    Taking DATE() of a UTC timestamp puts orders from 00:00-08:59 JST on the previous day, so
    always convert with this macro instead of calling date() directly.
-#}
{% macro jst_date(timestamp_column) -%}
    date({{ timestamp_column }}, 'Asia/Tokyo')
{%- endmacro %}

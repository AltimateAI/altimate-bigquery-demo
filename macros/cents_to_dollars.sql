{% macro cents_to_dollars(column_name, precision=2) %}
    round(cast({{ column_name }} as numeric) / 100, {{ precision }})
{% endmacro %}

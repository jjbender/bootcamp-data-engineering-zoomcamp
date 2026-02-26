{% macro safe_cast(field, type) %}
    {% if target.type == 'bigquery' %}
        safe_cast({{ field }} as {{ type }})
    {% elif target.type == 'duckdb' %}
        try_cast({{ field }} as {{ type }})
    {% else %}
        cast({{ field }} as {{ type }})
    {% endif %}
{% endmacro %}

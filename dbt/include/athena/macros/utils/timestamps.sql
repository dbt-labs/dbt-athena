{% macro athena__current_timestamp() -%}
    -- pyathena converts time zoned timestamps to strings so lets avoid them
    -- now()
    cast(now() as timestamp)
{%- endmacro %}

{% macro athena__end_of_time() -%}
    cast('9999-12-31' as timestamp)
{%- endmacro %}

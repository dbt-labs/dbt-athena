{% macro athena__current_timestamp() -%}
    -- pyathena converts time zoned timestamps to strings so lets avoid them
    -- now()
    cast(now() as timestamp)
{%- endmacro %}

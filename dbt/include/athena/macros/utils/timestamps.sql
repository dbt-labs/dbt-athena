{%
    pyathena converts time zoned timestamps to strings so lets avoid them now()
    Iceberg tables and Athena V3 will require a local overwrite of the athena__current_timestamp macro to timestamp(6))
%}

{% macro athena__current_timestamp() -%}
    cast(now() as timestamp)
{%- endmacro %}

{%
  Macro to get the end_of_time timestamp
%}

{% macro end_of_time() -%}
  {{ return(adapter.dispatch('end_of_time')()) }}
{%- endmacro %}

{% macro athena__end_of_time() -%}
  cast('9999-01-01' AS timestamp)
{%- endmacro %}

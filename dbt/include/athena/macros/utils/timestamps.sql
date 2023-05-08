{%
    pyathena converts time zoned timestamps to strings so lets avoid them now()
%}

{% macro athena__current_timestamp() -%}
  {{ cast_timestamp('now()') }}
{%- endmacro %}


{% macro cast_timestamp(timestamp_col) -%}
  {%- set config = model.get('config', {}) -%}
  {%- set table_type = config.get('table_type', 'glue') -%}
  {%- if table_type == 'iceberg' -%}
    cast({{ timestamp_col }} as timestamp(6))
  {%- else -%}
    cast({{ timestamp_col }} as timestamp)
  {%- endif -%}
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

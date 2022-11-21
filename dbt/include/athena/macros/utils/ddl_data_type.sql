{# Athena has different types between DML and DDL #}
{# ref: https://docs.aws.amazon.com/athena/latest/ug/data-types.html #}
{% macro ddl_data_type(col_type) -%}
    -- transform varchar
  {% set re = modules.re %}
  {% set data_type = re.sub('(?:varchar|character varying)(?:\(\d+\))?', 'string', col_type) %}

  -- transform array and map
  {%- if 'array' in data_type or 'map' in data_type -%}
    {% set data_type = data_type.replace('(', '<').replace(')', '>') -%}
  {%- endif -%}

  -- transform int
  {%- if 'integer' in data_type -%}
    {% set data_type = data_type.replace('integer', 'int') -%}
  {%- endif -%}

  {{ return(data_type) }}
{% endmacro %}

{# Athena has different types between DML and DDL #}
{# ref: https://docs.aws.amazon.com/athena/latest/ug/data-types.html #}
{% macro ddl_data_type(col_type) -%}
    -- transform varchar
  {% set re = modules.re %}
  {% set ns = namespace(ddl_type=col_type) %}
  {% set matching_strings = re.findall('(varchar\(\d+\)|character varying\(\d+\))', ns.ddl_type) %}
  {% for varchar_part in matching_strings %}
    {% set ns.ddl_type = ns.ddl_type.replace(varchar_part, 'string') %}
  {% endfor %}

  {% set data_type = ns.ddl_type -%}

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

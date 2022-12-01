{% macro validate_table_strategy(raw_strategy, format) %}
  {%- if format == 'iceberg' -%}
    {% set invalid_strategy_msg -%}
      Invalid table strategy provided: {{ raw_strategy }}
      Table models on Iceberg tables only work with 'tmp' or 'ctas_rename' (v3 only) strategy.
    {%- endset %}
    {% if raw_strategy not in ['tmp_parquet', 'ctas_rename'] %}
      {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
    {% endif %}
  {%- else -%}
    {% set invalid_strategy_msg -%}
      Invalid table strategy provided: {{ raw_strategy }}
      Expected 'ctas'
    {%- endset %}

    {% if raw_strategy != 'ctas' %}
      {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
    {% endif %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}

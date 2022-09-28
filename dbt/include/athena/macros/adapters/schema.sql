{% macro default__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier().render_hive() }}
  {% endcall %}
{% endmacro %}


{% macro default__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier().render_hive() }} cascade
  {% endcall %}
{% endmacro %}

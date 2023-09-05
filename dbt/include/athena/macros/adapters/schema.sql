{% macro athena__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier().render_hive() }}
  {% endcall %}

  {%- set lf_tags_database = config.get('lf_tags_database') -%}
  {% if lf_tags_database is not none %}
    {{ adapter.add_lf_tags_to_database(relation, lf_tags_database) }}
  {% endif %}

{% endmacro %}


{% macro athena__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier().render_hive() }} cascade
  {% endcall %}
{% endmacro %}

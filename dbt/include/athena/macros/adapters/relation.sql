{% macro athena__drop_relation(relation) -%}
  {% if config.get('incremental_strategy') != 'append' %}
    {%- do adapter.clean_up_table(relation.schema, relation.table) -%}
  {% endif %}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

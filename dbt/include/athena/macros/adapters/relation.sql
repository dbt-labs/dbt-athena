{% macro drop_relation(relation) -%}
  {% if config.get('format') != 'iceberg' and config.get('incremental_strategy') != 'append' %}
    {%- do adapter.clean_up_table(relation.schema, relation.table) -%}
  {% endif %}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro set_table_classification(relation) -%}
  {%- set format = config.get('format') -%}
  {% call statement('set_table_classification', auto_begin=False) -%}
    alter table {{ relation }} set tblproperties ('classification' = '{{ format }}')
  {%- endcall %}
{%- endmacro %}

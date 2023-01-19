{% macro athena__drop_relation(relation) -%}
  {% set rel_type = adapter.get_table_type(relation) %}
  {%- if rel_type is not none and rel_type == 'table' %}
    {%- do adapter.clean_up_table(relation.schema, relation.table) -%}
  {%- endif %}
    {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro set_table_classification(relation) -%}
  {%- set format = config.get('format', default='parquet') -%}
  {% call statement('set_table_classification', auto_begin=False) -%}
    alter table {{ relation }} set tblproperties ('classification' = '{{ format }}')
  {%- endcall %}
{%- endmacro %}

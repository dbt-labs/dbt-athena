{% macro athena__drop_relation(relation) -%}
  {% set rel_type = adapter.get_table_type(relation.schema, relation.table) %}
  {%- if rel_type is not none %}
    {%- if rel_type == 'table' %}
      {%- do adapter.clean_up_table(relation.schema, relation.table) -%}
    {%- endif %}
    {% call statement('drop_relation', auto_begin=False) -%}
      {%- if relation.type == 'view' -%}
        drop {{ relation.type }} if exists {{ relation.render() }}
      {%- else -%}
        drop {{ relation.type }} if exists {{ relation.render_hive() }}
      {% endif %}
    {%- endcall %}
  {%- endif %}
{% endmacro %}

{% macro set_table_classification(relation) -%}
  {%- set format = config.get('format', default='parquet') -%}
  {% call statement('set_table_classification', auto_begin=False) -%}
    alter table {{ relation.render_hive() }} set tblproperties ('classification' = '{{ format }}')
  {%- endcall %}
{%- endmacro %}

{% macro athena__rename_relation(from_relation, to_relation) %}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation.render_hive() }} rename to `{{ to_relation.schema }}`.`{{ to_relation.identifier }}`
  {%- endcall %}
{%- endmacro %}

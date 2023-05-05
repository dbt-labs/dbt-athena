{% macro athena__drop_relation(relation) -%}
  {% set rel_type_object = adapter.get_table_type(relation.schema, relation.identifier) %}
  {%- if rel_type_object is not none %}
    {% set rel_type = rel_type_object.value %}
    {%- if rel_type == 'table' or rel_type == 'iceberg_table' %}
      {%- do adapter.clean_up_table(relation.schema, relation.identifier) -%}
    {%- endif %}
    {%- do adapter.delete_from_glue_catalog(relation) -%}
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

{% macro drop_relation(relation, delay_seconds_before_s3_delete=none) -%}
  {% if config.get('table_type') != 'iceberg' %}
    {%- do adapter.clean_up_table(relation.schema, relation.table, delay_seconds_before_s3_delete) -%}
  {% endif %}
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

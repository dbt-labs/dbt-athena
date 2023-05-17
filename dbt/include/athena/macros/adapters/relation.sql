{% macro athena__drop_relation(relation) -%}
  {%- set native_drop = config.get('native_drop', default='False') | as_bool -%}

  {%- if native_drop -%}
    {%- do drop_relation_sql(relation) -%}
  {%- else -%}
    {%- do drop_relation_glue(relation) -%}
  {%- endif -%}
{% endmacro %}

{% macro drop_relation_glue(relation) -%}
  {%- do log('Dropping relation via Glue and S3 APIs') -%}
  {%- do adapter.clean_up_table(relation) -%}
  {%- do adapter.delete_from_glue_catalog(relation) -%}
{% endmacro %}

{% macro drop_relation_sql(relation) -%}
  {%- do log('Dropping relation via SQL only') -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {%- if relation.type == 'view' -%}
      drop {{ relation.type }} if exists {{ relation.render() }}
    {%- else -%}
      drop {{ relation.type }} if exists {{ relation.render_hive() }}
    {% endif %}
  {%- endcall %}
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

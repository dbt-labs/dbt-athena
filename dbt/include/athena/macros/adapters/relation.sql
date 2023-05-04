{% macro athena__drop_relation(relation) -%}
  {%- do adapter.clean_up_table(relation) -%}
  {%- do adapter.delete_from_glue_catalog(relation) -%}
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

{% macro athena__get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
  {%- set table_type = config.get('table_type') -%}

  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type,
      _table_type=table_type,
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}

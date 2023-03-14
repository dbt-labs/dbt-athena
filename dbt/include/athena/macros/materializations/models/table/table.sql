{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {%- set lf_tags = config.get('lf_tags', default=none) -%}
  {%- set table_type = config.get('table_type', default='hive') | lower -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- cleanup
  {%- if old_relation is not none -%}
    {{ drop_relation(old_relation) }}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {% if table_type != 'iceberg' %}
    {{ set_table_classification(target_relation) }}
  {% endif %}

  {% if lf_tags is not none %}
    {{ adapter.add_lf_tags(target_relation.schema, identifier, lf_tags) }}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

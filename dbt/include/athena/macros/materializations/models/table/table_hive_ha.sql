{% materialization table_hive_ha, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}
  {%- set table_type = 'hive' -%}

  {%- set s3_data_naming = config.get('s3_data_naming', default='table') -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- cleanup
  {%- if old_relation is not none -%}
    {% set unique_identifier = adapter.get_unique_identifier() %}
    {% set tmp_relation = make_temp_relation(target_relation, '_' + unique_identifier) %}

    {%- if tmp_relation is not none -%}
      {% call statement('drop_tmp_relation', auto_begin=False) -%}
        drop table if exists {{ tmp_relation }}
      {%- endcall %}
    {% endif %}
    -- create tmp table
    {% call statement('main') -%}
      {{ create_table_as(False, tmp_relation, sql) }}
    {%- endcall %}

    -- save the original target table location before swapping
    {% set original_table_location = adapter.get_table_location(target_relation.schema, target_relation.table) %}

    -- swap table
    {% set swap_table = adapter.swap_table(tmp_relation.schema, tmp_relation.name, target_relation.schema, target_relation.table) %}

    -- delete old table location
    {% set result_delete_location = adapter.prune_s3_table_location(original_table_location) %}

    -- TODO expire old table versions of target table -> this can be a macro

    {%- if tmp_relation is not none -%}
      {% call statement('drop_tmp_relation', auto_begin=False) -%}
        drop table if exists {{ tmp_relation }}
      {%- endcall %}
    {% endif %}

  {%- else -%}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}
  {% endif %}

  {{ set_table_classification(target_relation) }}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

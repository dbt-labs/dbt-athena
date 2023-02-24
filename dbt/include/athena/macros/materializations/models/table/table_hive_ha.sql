{% materialization table_hive_ha, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}
  {%- set table_type = 'hive' -%}
  {%- set versions_to_keep = config.get('versions_to_keep', default=4) -%}
  {%- set s3_data_dir = config.get('s3_data_dir', default=target.s3_data_dir) -%}
  {%- set s3_data_naming = config.get('s3_data_naming', default='table_unique') -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set full_refresh_config = config.get('full_refresh', default=False) -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True or full_refresh_config == True) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- if s3_data_naming in ['table', 'table_schema'] or external_location is not none -%}
    {%- set error_unique_location_hive_ha -%}
        You need to have an unique table location when using table_hive_ha materialization.
        Use s3_data_naming table_unique or schema_table_unique, and avoid to set an explicit external_location.
    {%- endset -%}
    {% do exceptions.raise_compiler_error(error_unique_location_hive_ha) %}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  -- in case of full refresh we drop the relation if exist
  -- the drop statement take care of deleting the data of the current version
  -- TODO create a drop_relation_with_versions, to be sure to remove all historical versions of a table
  {%- if full_refresh_mode and old_relation is not none-%}
    {{ drop_relation(old_relation) }}
  {%- endif -%}

  {%- if old_relation is none or full_refresh_mode-%}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}
  {%- else -%}
    {% set tmp_relation = make_temp_relation(target_relation, '__ha') %}

    -- drop the tmp_relation
    {% call statement('drop_tmp_relation', auto_begin=False) -%}
      drop table if exists {{ tmp_relation.render_hive() }}
    {%- endcall %}

    -- create tmp table
    {% call statement('main') -%}
      {{ create_table_as(False, tmp_relation, sql) }}
    {%- endcall %}

    -- save the original target table location before swapping
    {% set original_table_location = adapter.get_table_location(target_relation.schema, target_relation.table) %}

    -- swap table
    {% set swap_table = adapter.swap_table(tmp_relation.schema, tmp_relation.name, target_relation.schema, target_relation.table) %}

    -- delete glue tmp table, do not use drop_relation, as it will remove data of the target table
    {% call statement('drop_tmp_relation', auto_begin=False) -%}
      drop table if exists {{ tmp_relation.render_hive() }}
    {%- endcall %}

    {% set result_table_version_expiration = adapter.expire_glue_table_versions(target_relation.schema, target_relation.table, versions_to_keep, True) %}

  {% endif %}

  {{ set_table_classification(target_relation) }}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

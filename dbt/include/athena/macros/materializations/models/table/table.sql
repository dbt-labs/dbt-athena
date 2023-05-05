-- TODO create a drop_relation_with_versions, to be sure to remove all historical versions of a table
{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {%- set lf_tags = config.get('lf_tags', default=none) -%}
  {%- set lf_tags_columns = config.get('lf_tags_columns', default=none) -%}
  {%- set table_type = config.get('table_type', default='hive') | lower -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set is_ha = config.get('ha', default=false) -%}
  {%- set s3_data_dir = config.get('s3_data_dir', default=target.s3_data_dir) -%}
  {%- set s3_data_naming = config.get('s3_data_naming', default='table_unique') -%}
  {%- set full_refresh_config = config.get('full_refresh', default=False) -%}
  {%- set is_full_refresh_mode = (flags.FULL_REFRESH == True or full_refresh_config == True) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {%- set tmp_relation = api.Relation.create(identifier=target_relation.identifier ~ '__ha',
                                             schema=schema,
                                             database=database,
                                             s3_path_table_part=target_relation.identifier,
                                             type='table') -%}

  {%- if (
    table_type == 'hive'
    and is_ha
    and (s3_data_naming in ['table', 'schema_table'] or external_location is not none)
  ) -%}
      {%- set error_unique_location_hive_ha -%}
          You need to have an unique table location when using ha config with hive table.
          Use s3_data_naming table_unique or schema_table_unique, and avoid to set an explicit
          external_location.
      {%- endset -%}
      {% do exceptions.raise_compiler_error(error_unique_location_hive_ha) %}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  {%- if table_type == 'hive' -%}

    {%- if is_ha and not is_full_refresh_mode -%}
      -- drop the tmp_relation
      {%- if tmp_relation is not none -%}
        {% call statement('drop_tmp_relation', auto_begin=False) -%}
          drop table if exists {{ tmp_relation.render_hive() }}
        {%- endcall %}
      {%- endif -%}

      -- create tmp table
      {% call statement('main') -%}
        {{ create_table_as(False, tmp_relation, sql) }}
      {%- endcall %}

      -- swap table
      {%- set swap_table = adapter.swap_table(tmp_relation,
                                              target_relation) -%}

      -- delete glue tmp table, do not use drop_relation, as it will remove data of the target table
      {% call statement('drop_tmp_relation', auto_begin=False) -%}
        drop table {{ tmp_relation.render_hive() }}
      {%- endcall %}

      {% do adapter.expire_glue_table_versions(target_relation,
                                               versions_to_keep,
                                               True) %}
    {%- else -%}
      -- Here we are in the case of non-ha tables or ha tables but in case of full refresh.
      {%- if old_relation is not none -%}
        {{ drop_relation(old_relation) }}
      {%- endif -%}
      {%- call statement('main') -%}
        {{ create_table_as(False, target_relation, sql) }}
      {%- endcall %}
    {%- endif -%}

    {{ set_table_classification(target_relation) }}

  {%- else -%}

    {%- if old_relation is none -%}
      {%- call statement('main') -%}
        {{ create_table_as(False, target_relation, sql) }}
      {%- endcall %}
    {%- else -%}
      {%- if tmp_relation is not none -%}
        {%- do drop_relation(tmp_relation) -%}
      {%- endif -%}

      {%- set old_relation_bkp = make_temp_relation(old_relation, '__bkp') -%}
      -- If we have this, it means that at least the first renaming occurred but there was an issue
      -- afterwards, therefore we are in weird state. The easiest and cleanest should be to remove
      -- the backup relation. It won't have an impact because since we are in the else condition,
      -- that means that old relation exists therefore no downtime yet.
      {%- if old_relation_bkp is not none -%}
        {%- do drop_relation(old_relation_bkp) -%}
      {%- endif -%}

      {%- call statement('main') -%}
        {{ create_table_as(False, tmp_relation, sql) }}
      {%- endcall -%}

      {{ rename_relation(old_relation, old_relation_bkp) }}
      {{ rename_relation(tmp_relation, target_relation) }}

      {{ drop_relation(old_relation_bkp) }}
    {%- endif -%}

  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {% if lf_tags is not none or lf_tags_columns is not none %}
    {{ adapter.add_lf_tags(target_relation.schema, identifier, lf_tags, lf_tags_columns) }}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

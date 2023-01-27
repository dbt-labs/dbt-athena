{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {% set existing_relation = load_relation(this) %}

  {%- set table_type = config.get('table_type', default='hive') | lower -%}
  {%- set s3_data_naming = config.get('s3_data_naming', default=target.s3_data_naming) -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set s3_data_dir = config.get('s3_data_dir', default=target.s3_data_dir) -%}
  {%- set s3_data_naming = config.get('s3_data_naming', default=target.s3_data_naming) -%}
  {%- set zero_downtime_delete_delay_seconds = config.get('zero_downtime_delete_delay_seconds', default=target.zero_downtime_delete_delay_seconds) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set location = adapter.s3_table_location(s3_data_dir, s3_data_naming, target_relation.schema, target_relation.identifier, external_location, False) -%}

  {{ run_hooks(pre_hooks) }}

  -- cleanup
  {%- if old_relation is not none and (s3_data_naming not in ['schema_table_unique', 'table_unique'] or external_location is not none or should_full_refresh()) -%}
    {{ drop_relation(old_relation) }}
  {%- endif -%}

  {%- if existing_relation is not none and s3_data_naming in ['schema_table_unique', 'table_unique'] and external_location is none and not should_full_refresh() -%}
    {% set relation_to_build = make_temp_relation(target_relation) %}
  {%- else -%}
    {%- set relation_to_build = target_relation -%}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, relation_to_build, location, sql) }}
  {%- endcall %}
  {%- if existing_relation is not none and s3_data_naming in ['schema_table_unique', 'table_unique'] and external_location is none and not should_full_refresh() -%}
    {%- set original_schema = adapter.get_columns_in_relation(target_relation) -%}
    {%- set updated_schema = adapter.get_columns_in_relation(relation_to_build) -%}
    {%- if original_schema != updated_schema -%}
      -- Drop original table
      {% do drop_relation(target_relation) %}
      {% set build_sql = create_table_as(False, target_relation, none, 'select * from ' ~ relation_to_build.schema ~ '.' ~ relation_to_build.identifier ~ ' with no data') -%}
      {% call statement("build_sql") %}
        {{ build_sql }}
      {% endcall %}
    {%- endif -%}
  {%- endif -%}

  {%- if existing_relation is not none and s3_data_naming in ['schema_table_unique', 'table_unique'] and external_location is none and not should_full_refresh() -%}
    {# get location of existing table #}
    {%- set existing_location = adapter.get_table_location(target_relation.schema, target_relation.identifier) -%}
    -- redirect s3 location of existing table to new location
    {% call statement('alter_table_location_existing_table') -%}
      {{ alter_table_location(target_relation, location) }}
    {%- endcall %}
    -- redirect s3 location of tmp table to old location
    {% call statement('alter_table_location_tmp_table') -%}
      {{ alter_table_location(relation_to_build, existing_location) }}
    {%- endcall %}
    -- drop temporary table (and prune old location after a delay)
    {% do drop_relation(relation_to_build, zero_downtime_delete_delay_seconds) %}
  {%- endif -%}

  {% if table_type != 'iceberg' %}
    {{ set_table_classification(target_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{%
  Hash function to generate dbt_scd_id. dbt by default uses md5(coalesce(cast(field as varchar)))
   but it does not work with athena. It throws an error Unexpected parameters (varchar) for function
   md5. Expected: md5(varbinary)
%}

{% macro athena__snapshot_hash_arguments(args) -%}
    to_hex(md5(to_utf8({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})))
{%- endmacro %}

{%
  Update the standard snapshot merge to include the DBT_INTERNAL_SOURCE prefix in the src_cols_csv
%}

{% macro athena__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}
    {%- set src_columns = [] -%}
    {%- for col in insert_cols -%}
      {%- do src_columns.append('DBT_INTERNAL_SOURCE.' + col) -%}
    {%- endfor -%}
    {%- set src_cols_csv = src_columns | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ src_cols_csv }})
{% endmacro %}

{%- macro snapshot_get_time() -%}
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))
{%- endmacro -%}

{%
  Create the snapshot table from the source with dbt snapshot columns
%}

{% macro build_snapshot_table(strategy, sql) %}
    select *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq
{% endmacro %}

{%
  Identify records that needs to be upserted or deleted into the snapshot table
%}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}
    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id

        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}

{%
  Create a new temporary table to hold the records to be upserted or deleted
%}

{% macro athena__build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(True, temp_relation, select) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}

{#
    Add new columns to the table if applicable
#}

{% macro athena__create_columns(relation, columns) -%}
  {%- for column in columns -%}
    {% if column.data_type|lower == 'boolean' %}
    {% set query -%}
      ALTER TABLE {{ relation }} ADD COLUMNS ({{ column.name }} BOOLEAN);
    {%- endset -%}
    {% elif column.data_type|lower == 'character varying(256)' %}
    {% set query -%}
      ALTER TABLE {{ relation }} ADD COLUMNS ({{ column.name }} VARCHAR);
    {%- endset -%}
    {% elif column.data_type|lower == 'integer' %}
    {% set query -%}
      ALTER TABLE {{ relation }} ADD COLUMNS ({{ column.name }} INT);
    {%- endset -%}
    {% elif column.data_type|lower == 'float' %}
    {% set query -%}
      ALTER TABLE {{ relation }} ADD COLUMNS ({{ column.name }} FLOAT);
    {%- endset -%}
    {% else %}
      ALTER TABLE {{ relation }} ADD COLUMNS ({{ column.name }} {{ column.data_type }});
    {% endif %}
    {% do run_query(query) %}
  {%- endfor %}
{% endmacro %}



{% materialization snapshot, adapter='athena' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set file_format = config.get('file_format', 'parquet') -%}
  {%- set table_type = config.get('table_type', 'iceberg') -%}

  {% if table_type != 'iceberg' %}
      {{ exceptions.raise_not_implemented(
    'snapshot materialization not implemented for any table type except iceberg')
    }}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=none,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}


  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = athena__build_snapshot_staging_table(strategy, sql, target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = athena__snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% if staging_table is defined %}
      {% do adapter.drop_relation(staging_table) %}
  {% endif %}


  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

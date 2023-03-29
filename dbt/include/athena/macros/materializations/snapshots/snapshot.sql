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
    If hive table then Recreate the snapshot table from the new_snapshot_table
    If iceberg table then Update the standard snapshot merge to include the DBT_INTERNAL_SOURCE prefix in the src_cols_csv
%}

{% macro athena__snapshot_merge_sql(target, source, insert_cols, table_type) -%}
    {% if table_type=='iceberg' %}
        {%- set insert_cols_csv = insert_cols | join(', ') -%}
        {%- set src_columns = [] -%}
        {%- for col in insert_cols -%}
          {%- do src_columns.append('dbt_internal_source.' + col) -%}
        {%- endfor -%}
        {%- set src_cols_csv = src_columns | join(', ') -%}

        merge into {{ target }} as dbt_internal_dest
        using {{ source }} as dbt_internal_source
        on dbt_internal_source.dbt_scd_id = dbt_internal_dest.dbt_scd_id

        when matched
         and dbt_internal_dest.dbt_valid_to is null
         and dbt_internal_source.dbt_change_type in ('update', 'delete')
            then update
            set dbt_valid_to = dbt_internal_source.dbt_valid_to

        when not matched
         and dbt_internal_source.dbt_change_type = 'insert'
            then insert ({{ insert_cols_csv }})
            values ({{ src_cols_csv }})
    {%else%}
        {%- set target_relation = adapter.get_relation(database=target.database, schema=target.schema, identifier=target.identifier) -%}
        {%- if target_relation is not none -%}
          {% do adapter.drop_relation(target_relation) %}
        {%- endif -%}

        {% set sql -%}
          select * from {{ source }};
        {%- endset -%}

        {{ create_table_as(False, target_relation, sql) }}
    {% endif %}
{% endmacro %}


{%
  Create the snapshot table from the source with dbt snapshot columns
%}

{% macro build_snapshot_table(strategy, source_sql, table_type) %}
    {% if table_type=='iceberg' %}
        select *,
            {{ strategy.scd_id }} as dbt_scd_id,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
        from (
            {{ source_sql }}
        ) sbq
    {%else%}
        select *
          , {{ strategy.unique_key }} as dbt_unique_key
          , {{ strategy.updated_at }} as dbt_valid_from
          , {{ strategy.scd_id }} as dbt_scd_id
          , 'insert' as dbt_change_type
          , {{ end_of_time() }} as dbt_valid_to
          , True as is_current_record
          , {{ strategy.updated_at }} as dbt_snapshot_at
        from ({{ source_sql }}) source;
    {% endif %}
{% endmacro %}

{%
  Identify records that needs to be upserted or deleted into the snapshot table
%}


{% macro snapshot_staging_table(strategy, source_sql, target_relation, table_type) -%}
    {% if table_type=='iceberg' %}
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
    {%else%}
        with snapshot_query as (
            {{ source_sql }}
        )
        , snapshotted_data_base as (
            select *
              , ROW_NUMBER() OVER (
                  PARTITION BY dbt_unique_key
                  ORDER BY dbt_valid_from DESC
                ) as dbt_snapshot_rn
            from {{ target_relation }}
        )
        , snapshotted_data as (
            select *
            from snapshotted_data_base
            where dbt_snapshot_rn = 1
              AND dbt_change_type != 'delete'
        )
        , source_data as (
            select *
              , {{ strategy.unique_key }} as dbt_unique_key
              , {{ strategy.updated_at }} as dbt_valid_from
              , {{ strategy.scd_id }} as dbt_scd_id
            from snapshot_query
        )
        , upserts as (
            select source_data.*
              , case
                  when snapshotted_data.dbt_unique_key IS NULL THEN 'insert'
                  else 'update'
                end as dbt_change_type
              , {{ end_of_time() }} as dbt_valid_to
              , True as is_current_record
            from source_data
            LEFT join snapshotted_data
                   on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
            where snapshotted_data.dbt_unique_key IS NULL
               OR (
                    snapshotted_data.dbt_unique_key IS NOT NULL
                AND (
                    {{ strategy.row_changed }}
                )
            )
        )
        {%- if strategy.invalidate_hard_deletes -%}
          , deletes as (
              select
                  source_data.*
                , 'delete' as dbt_change_type
                , {{ end_of_time() }} as dbt_valid_to
                , True as is_current_record
              from snapshotted_data
              LEFT join source_data
                     on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
              where source_data.dbt_unique_key IS NULL
            )
          select * from upserts
          union all
          select * from deletes;
        {% else %}
        select * from upserts;
        {% endif %}
    {% endif %}
{%- endmacro %}

{%
  Create a new temporary table to hold the records to be upserted or deleted
%}

{% macro athena__build_snapshot_staging_table(strategy, sql, target_relation, table_type) %}
    {% if table_type=='iceberg' %}
        {% set temp_relation = make_temp_relation(target_relation) %}

        {% set select = snapshot_staging_table(strategy, sql, target_relation, table_type) %}

        {% call statement('build_snapshot_staging_relation') %}
            {{ create_table_as(True, temp_relation, select) }}
        {% endcall %}

        {% do return(temp_relation) %}
    {%else%}
        {%- set tmp_identifier = target_relation.identifier ~ '__dbt_tmp' -%}

        {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=target_relation.schema,
                                                      database=target_relation.database,
                                                      type='table') -%}

        {% do adapter.drop_relation(tmp_relation) %}

        {%- set select = snapshot_staging_table(strategy, sql, target_relation, table_type) -%}

        {% call statement('build_snapshot_staging_relation') %}
            {{ create_table_as(False, tmp_relation, select) }}
        {% endcall %}

        {% do return(tmp_relation) %}
    {% endif %}
{% endmacro %}

{#
    Update the dbt_valid_to and is_current_record for
    snapshot rows being updated and create a new temporary table to hold them
#}

{% macro athena__create_new_snapshot_table(strategy, strategy_name, target, source) %}
    {%- set tmp_identifier = target.identifier ~ '__dbt_tmp_1' -%}

    {%- set tmp_relation = adapter.get_relation(database=target.database, schema=target.schema, identifier=tmp_identifier) -%}

    {%- set target_relation = api.Relation.create(identifier=tmp_identifier,
      schema=target.schema,
      database=target.database,
      type='table') -%}

    {%- set source_columns = adapter.get_columns_in_relation(source) -%}

    {% if tmp_relation is not none %}
      {% do adapter.drop_relation(tmp_relation) %}
    {% endif %}

    {% set sql -%}
      select
        {% for column in source_columns %}
          {{ column.name }} {%- if not loop.last -%},{%- endif -%}
        {% endfor %}
        ,dbt_snapshot_at
      from {{ target }}
      where dbt_unique_key NOT IN ( select dbt_unique_key from {{ source }} )
      union all
      select
        {% for column in source_columns %}
          {% if column.name == 'dbt_valid_to' %}
          case
          when tgt.is_current_record
          THEN
          {% if strategy_name == 'timestamp' %}
            src.{{ strategy.updated_at }}
          {% else %}
            {{ strategy.updated_at }}
          {% endif %}
          else tgt.dbt_valid_to
          end as dbt_valid_to {%- if not loop.last -%},{%- endif -%}
          {% elif column.name == 'is_current_record' %}
          False as is_current_record {%- if not loop.last -%},{%- endif -%}
          {% else %}
            tgt.{{ column.name }} {%- if not loop.last -%},{%- endif -%}
          {% endif %}
        {% endfor %}
        ,
        {% if strategy_name == 'timestamp' %}
            tgt.{{ strategy.updated_at }}
        {% else %}
          {{ strategy.updated_at }}
        {% endif %} as dbt_snapshot_at
      from {{ target }} tgt
      join {{ source }} src
      on tgt.dbt_unique_key = src.dbt_unique_key
      union all
      select
        {% for column in source_columns %}
            {{ column.name }} {%- if not loop.last -%},{%- endif -%}
        {% endfor %}
        ,{{ strategy.updated_at }} as dbt_snapshot_at
      from {{ source }};
    {%- endset -%}

    {% call statement('create_new_snapshot_table') %}
        {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}

    {% do return(target_relation) %}
{% endmacro %}

{% materialization snapshot, adapter='athena' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set file_format = config.get('file_format', 'parquet') -%}
  {%- set table_type = config.get('table_type', 'iceberg') -%}


  {{ log('Checking if target table exists') }}
  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
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

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql'], table_type) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}


      {% set staging_table = athena__build_snapshot_staging_table(strategy, sql, target_relation, table_type) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}


      {% if missing_columns %}
        {% do alter_relation_add_columns(target_relation, missing_columns) %}
      {% endif %}


      {% if table_type == 'iceberg' %}
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
      {% endif %}

      {% if table_type == 'iceberg' %}
          {% set final_sql = athena__snapshot_merge_sql(
                target = target_relation,
                source = staging_table,
                insert_cols = quoted_source_columns,
                table_type = table_type
             )
          %}
      {% else %}
          {% set new_snapshot_table = athena__create_new_snapshot_table(strategy, strategy_name, target_relation, staging_table) %}
          {% set final_sql = athena__snapshot_merge_sql(
                target = target_relation,
                source = new_snapshot_table
             )
          %}
      {% endif %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% if staging_table is defined %}
      {% do adapter.drop_relation(staging_table) %}
  {% endif %}

  {% if new_snapshot_table is defined %}
      {% do adapter.drop_relation(new_snapshot_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

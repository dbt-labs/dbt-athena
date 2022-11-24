{% materialization incremental, adapter='athena' -%}

  {% set raw_strategy = config.get('incremental_strategy') or 'insert_overwrite' %}
  {% set format = config.get('format', default='parquet') %}
  {% set strategy = validate_get_incremental_strategy(raw_strategy, format) %}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {% set partitioned_by = config.get('partitioned_by', default=none) %}
  {% set staging_location = config.get('staging_location') %}
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Having Iceberg in v2 engine not able to do CTAS makes it complex. Hard to factorize with table materialization
  -- because of statement block.
  {%- if format == 'iceberg' -%}
    {%- set iceberg_tmp_relation = make_temp_relation(target_relation, '__incremental_iceberg_ctas') -%}
  {%- endif -%}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
    {%- if format == 'iceberg' -%}
      {%- set build_sql = create_table_iceberg(target_relation, existing_relation, iceberg_tmp_relation, sql) -%}
      {% do to_drop.append(iceberg_tmp_relation) %}
    {% else %}
      {% set build_sql = create_table_as(False, target_relation, sql) -%}
    {%- endif -%}
  {% elif existing_relation.is_view or should_full_refresh() %}
    {% do adapter.drop_relation(existing_relation) %}
    {%- if format == 'iceberg' -%}
      {%- set build_sql = create_table_iceberg(target_relation, existing_relation, iceberg_tmp_relation, sql) -%}
      {% do to_drop.append(iceberg_tmp_relation) %}
    {% else %}
      {% set build_sql = create_table_as(False, target_relation, sql) -%}
    {%- endif -%}
  {% elif partitioned_by is not none and strategy == 'insert_overwrite' %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% if tmp_relation is not none %}
        {% do adapter.drop_relation(tmp_relation) %}
      {% endif %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do delete_overlapping_partitions(target_relation, tmp_relation, partitioned_by) %}
      {% set build_sql = incremental_insert(on_schema_change, tmp_relation, target_relation, existing_relation) %}
      {% do to_drop.append(tmp_relation) %}
  {% elif strategy == 'append' %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% if tmp_relation is not none %}
        {% do adapter.drop_relation(tmp_relation) %}
      {% endif %}
      {%- if format == 'iceberg' -%}
         -- We need to use the create_tmp_table_iceberg macro (parquet table) because create_table_as create a table
         -- with the format inherited from config. For Iceberg, what we are doing is that we create a parquet table
         -- containing the result of the incremental query (no limit), and then we insert this in the iceberg table.
        {% do run_query(create_tmp_table_iceberg(tmp_relation, sql, staging_location, false)) %}
      {% else %}
        {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {%- endif -%}
      {% set build_sql = incremental_insert(on_schema_change, tmp_relation, target_relation, existing_relation) %}
      {% do to_drop.append(tmp_relation) %}
  {% elif strategy == 'merge' and format == 'iceberg' %}
    -- Merge is only supported for Iceberg in v3 engine. In this scenario, we are going to start the same as the append
    -- strategy, meaning creating a temp table with the incremental query and then, instead of inserting, we are
    -- merging
    {% set unique_key = config.get('unique_key') %}
    {% set empty_unique_key -%}
      Merge strategy must implement unique_key as a single column or a list of columns.
    {%- endset %}
    {% if unique_key is none %}
      {% do exceptions.raise_compiler_error(empty_unique_key) %}
    {% endif %}

    {% set tmp_relation = make_temp_relation(target_relation) %}
    {% if tmp_relation is not none %}
      {% do adapter.drop_relation(tmp_relation) %}
    {% endif %}
    {% do run_query(create_tmp_table_iceberg(tmp_relation, sql, staging_location, false)) %}
    {% set build_sql = iceberg_merge(tmp_relation, target_relation, unique_key) %}
    {% do to_drop.append(tmp_relation) %}
  {% endif %}

  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  -- set table properties
  {% if not to_drop %}
    {{ set_table_classification(target_relation, 'parquet') }}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
    {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

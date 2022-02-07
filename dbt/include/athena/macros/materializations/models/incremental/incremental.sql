{% materialization incremental, adapter='athena' -%}

  {% set unique_key = config.get('unique_key') %}
  {% set overwrite_msg -%}
    Athena adapter does not support 'unique_key'
  {%- endset %}
  {% if unique_key is not none %}
    {% do exceptions.raise_compiler_error(overwrite_msg) %}
  {% endif %}

  {% set raw_strategy = config.get('incremental_strategy', default='insert_overwrite') %}
  {% set strategy = validate_get_incremental_strategy(raw_strategy) %}

  {% set partitioned_by = config.get('partitioned_by', default=none) %}
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or should_full_refresh() %}
      {% do adapter.drop_relation(existing_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif partitioned_by is not none and strategy == 'insert_overwrite' %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% if tmp_relation is not none %}
          {% do adapter.drop_relation(tmp_relation) %}
      {% endif %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do delete_overlapping_partitions(target_relation, tmp_relation, partitioned_by) %}
      {% set build_sql = incremental_insert(tmp_relation, target_relation) %}
      {% do to_drop.append(tmp_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% if tmp_relation is not none %}
          {% do adapter.drop_relation(tmp_relation) %}
      {% endif %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% set build_sql = incremental_insert(tmp_relation, target_relation) %}
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

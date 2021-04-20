
{% macro incremental_insert(tmp_relation, target_relation, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}

{% materialization incremental, adapter='athena' -%}

  {% set unique_key = config.get('unique_key') %}

  {% set overwrite_msg -%}
    Athena adapter does not support 'unique_key'
  {%- endset %}
  {% if unique_key is not none %}
    {% do exceptions.raise_compiler_error(overwrite_msg) %}
  {% endif %}

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
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
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

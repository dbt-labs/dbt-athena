{% macro iceberg_merge(on_schema_change, tmp_relation, target_relation, unique_key, existing_relation, statement_name="main") %}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {% endif %}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% if unique_key is sequence and unique_key is not string %}
      {%- set unique_key_cols = unique_key -%}
    {% else %}
      {%- set unique_key_cols = [unique_key] -%}
    {% endif %}
    {%- set src_columns = [] -%}
    {%- set update_columns = [] -%}
    {%- for col in dest_columns -%}
      {%- do src_columns.append('src."' + col.name + '"') -%}
      {% if col.name not in unique_key_cols %}
        {%- do update_columns.append(col.name) -%}
      {% endif %}
    {%- endfor -%}
    {%- set src_cols_csv = src_columns | join(', ') -%}

    merge into {{ target_relation }} as target using {{ tmp_relation }} as src
    ON (
      {% for key in unique_key_cols %}
        target.{{ key }} = src.{{ key }}
        {{ "and " if not loop.last }}
      {% endfor %}
    )
    when matched
      then update set
        {% for col in update_columns %}
          {{ '"' + col + '"' }} = {{ 'src."' + col + '"' }} {{ "," if not loop.last }}
        {% endfor %}
    when not matched
      then insert ({{ dest_cols_csv }})
       values ({{ src_cols_csv }});
{%- endmacro %}

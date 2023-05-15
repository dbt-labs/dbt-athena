{% macro sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {% set table_type = config.get('table_type', default='hive') | lower %}
  {%- if partitioned_by is none -%}
      {%- set partitioned_by = [] -%}
  {%- endif %}
  {%- set add_to_target_arr = schema_changes_dict['source_not_in_target'] -%}
  {%- if on_schema_change == 'append_new_columns'-%}
     {%- if add_to_target_arr | length > 0 -%}
       {%- do alter_relation_add_columns(target_relation, add_to_target_arr) -%}
     {%- endif -%}
  {% elif on_schema_change == 'sync_all_columns' %}
     {%- set remove_from_target_arr = schema_changes_dict['target_not_in_source'] -%}
     {%- set new_target_types = schema_changes_dict['new_target_types'] -%}
     {% if table_type == 'iceberg' %}
       {% if add_to_target_arr | length > 0 %}
         {%- do alter_relation_add_columns(target_relation, add_to_target_arr) -%}
       {% endif %}
       {% if remove_from_target_arr | length > 0 %}
         {%- do alter_relation_drop_columns(target_relation, remove_from_target_arr) -%}
       {% endif %}
       {% if new_target_types != [] %}
         {% for ntt in new_target_types %}
           {% set column_name = ntt['column_name'] %}
           {% set new_type = ntt['new_type'] %}
           {% do alter_column_type(target_relation, column_name, new_type) %}
         {% endfor %}
       {% endif %}
     {% else %}
       {%- set replace_with_target_arr = remove_partitions_from_columns(schema_changes_dict['source_columns'], partitioned_by) -%}
       {% if add_to_target_arr | length > 0 or remove_from_target_arr | length > 0 or new_target_types | length > 0 %}
         {%- do alter_relation_replace_columns(target_relation, replace_with_target_arr) -%}
       {% endif %}
     {% endif %}
  {% endif %}
  {% set schema_change_message %}
    In {{ target_relation }}:
        Schema change approach: {{ on_schema_change }}
        Columns added: {{ add_to_target_arr }}
        Columns removed: {{ remove_from_target_arr }}
        Data types changed: {{ new_target_types }}
  {% endset %}
  {% do log(schema_change_message) %}
{% endmacro %}

{% macro athena__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {# Fix bug in Athena: error when using quoted table name #}
  {%- set relation = relation.render_pure() -%}

  {# Fix bug in Athena: iceberg alter statements work requires "string" column type instead of "varchar" #}
  {%- set new_iceberg_column_type = new_column_type | replace('varchar', 'string') -%}

  {# Fix bug in Athena: iceberg alter statements work requires "timestamp" column type instead of "timestamp(X)" #}
  {#
    Note: if it needs to convert varchar column type with timestamp values to date,
    you need to convert it to timestamp type or update with truncating timestamp values to date
  #}
  {%- if 'timestamp' in new_column_type -%}
    {%- set new_iceberg_column_type = 'timestamp' -%}
  {%- endif -%}

  {%- set add_column_query -%}
    alter table {{ relation }} add columns({{ tmp_column }} {{ new_iceberg_column_type }});
  {%- endset -%}

  {%- set update_query -%}
    update {{ relation }} set {{ tmp_column }} = cast({{ column_name }} as {{ new_column_type }});
  {%- endset -%}

  {%- set drop_column_query -%}
    alter table {{ relation }} drop column {{ column_name }};
  {%- endset -%}

  {%- set rename_column_query -%}
    alter table {{ relation }} change column {{ tmp_column }} {{ column_name }} {{ new_iceberg_column_type }};
  {%- endset -%}

  {%- do run_query(add_column_query) -%}
  {%- do run_query(update_query) -%}
  {%- do run_query(drop_column_query) -%}
  {%- do run_query(rename_column_query) -%}

{% endmacro %}

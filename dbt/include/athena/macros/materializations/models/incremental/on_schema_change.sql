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

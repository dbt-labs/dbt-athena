{% macro alter_table_location(relation, new_location) -%}
  alter table {{relation.schema}}.{{ relation.identifier }} set location '{{ new_location }}'
{% endmacro %}

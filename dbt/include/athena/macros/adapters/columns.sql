{% macro athena__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

      select
          column_name,
          regexp_replace(replace(replace(data_type, 'varbinary', 'binary'), 'row', 'struct'), '(timestamp)(\(\d+\))(.*)', '$1$2') as data_type,,
          null as character_maximum_length,
          null as numeric_precision,
          null as numeric_scale

      from {{ relation.information_schema('columns') }}
      where table_name = LOWER('{{ relation.identifier }}')
        {% if relation.schema %}
            and table_schema = LOWER('{{ relation.schema }}')
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {% do return(sql_convert_columns_in_relation(table)) %}
{% endmacro %}

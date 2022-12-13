{% macro athena__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

      select
          column_name,
          replace(regexp_replace(regexp_replace(replace(replace(data_type, 'row(', 'struct<'), ')', '>'), '(\d)(>)', '$1)'), '(\w+)(\s)(\w+)', '$1:$3'), 'varbinary', 'binary') as data_type,,
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

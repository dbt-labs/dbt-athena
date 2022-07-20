{% macro athena__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

      select
          column_name,
          data_type,
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

{% macro alter_relation_add_columns(relation, add_columns = none) -%}
  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}

  {% set sql -%}
      alter {{ relation.type }} {{ relation }}
          add columns (
            {%- for column in add_columns -%}
                {{ column.name }} {{ column.data_type }}{{ ', ' if not loop.last }}
            {%- endfor -%}
          )
  {%- endset -%}

  {% if (add_columns | length) > 0 %}
    {{ return(run_query(sql)) }}
  {% endif %}
{% endmacro %}

{% macro alter_relation_replace_columns(relation, replace_columns = none) -%}
  {% if replace_columns is none %}
    {% set replace_columns = [] %}
  {% endif %}

  {% set sql -%}
      alter {{ relation.type }} {{ relation }}
          replace columns (
            {%- for column in replace_columns -%}
                {{ column.name }} {{ column.data_type }}{{ ', ' if not loop.last }}
            {%- endfor -%}
          )
  {%- endset -%}

  {% if (replace_columns | length) > 0 %}
    {{ return(run_query(sql)) }}
  {% endif %}
{% endmacro %}

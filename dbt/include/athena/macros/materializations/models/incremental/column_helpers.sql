{% macro alter_relation_add_columns(relation, add_columns = none) -%}
  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}

  {% set sql -%}
      alter {{ relation.type }} {{ relation }}
          add columns (
            {%- for column in add_columns -%}
                {{ column.name }} {{ ddl_data_type(column.data_type) }}{{ ', ' if not loop.last }}
            {%- endfor -%}
          )
  {%- endset -%}

  {% if (add_columns | length) > 0 %}
    {{ return(run_query(sql)) }}
  {% endif %}
{% endmacro %}

{% macro alter_relation_drop_columns(relation, remove_columns = none) -%}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}

  {%- for column in remove_columns -%}
    {% set sql -%}
      alter {{ relation.type }} {{ relation }} drop column {{ column.name }}
    {% endset %}
    {% do run_query(sql) %}
  {%- endfor -%}
{% endmacro %}

{% macro alter_relation_replace_columns(relation, replace_columns = none) -%}
  {% if replace_columns is none %}
    {% set replace_columns = [] %}
  {% endif %}

  {% set sql -%}
      alter {{ relation.type }} {{ relation }}
          replace columns (
            {%- for column in replace_columns -%}
                {{ column.name }} {{ ddl_data_type(column.data_type) }}{{ ', ' if not loop.last }}
            {%- endfor -%}
          )
  {%- endset -%}

  {% if (replace_columns | length) > 0 %}
    {{ return(run_query(sql)) }}
  {% endif %}
{% endmacro %}

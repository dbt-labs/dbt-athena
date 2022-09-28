{% macro set_table_classification(relation, default_value) -%}
    {%- set format = config.get('format', default=default_value) -%}

    {% call statement('set_table_classification', auto_begin=False) -%}
        alter table {{ relation.render_hive() }} set tblproperties ('classification' = '{{ format }}')
    {%- endcall %}
{%- endmacro %}

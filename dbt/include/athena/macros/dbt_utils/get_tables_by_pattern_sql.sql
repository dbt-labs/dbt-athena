{% macro athena__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}

    {% set sql %}
        select
            table_schema as "table_schema",
            table_name as "table_name",
            {{ dbt_utils.get_table_types_sql() }}
        from {{ database }}.information_schema.tables
        where table_schema like '{{ schema_pattern|lower }}'
            and table_name like '{{ table_pattern|lower }}'
            and table_name not like '{{ exclude|lower }}'
    {% endset %}

    {{ return(sql) }}

{% endmacro %}

{% macro athena__get_catalog(information_schema, schemas) -%}
    {%- set query -%}
        select * from (
            (
                with tables as (

                    select
                        tables.table_catalog as table_database,
                        tables.table_schema as table_schema,
                        tables.table_name as table_name,

                        case
                            when views.table_name is not null
                                then 'view'
                            when table_type = 'BASE TABLE'
                                then 'table'
                            else table_type
                        end as table_type,

                        null as table_comment

                    from {{ information_schema }}.tables
                    left join {{ information_schema }}.views
                        on tables.table_catalog = views.table_catalog
                        and tables.table_schema = views.table_schema
                        and tables.table_name = views.table_name

                ),

                columns as (

                    select
                        table_catalog as table_database,
                        table_schema as table_schema,
                        table_name as table_name,
                        column_name as column_name,
                        ordinal_position as column_index,
                        data_type as column_type,
                        comment as column_comment

                    from {{ information_schema }}.columns

                ),

                catalog as (

                    select
                        tables.table_database,
                        tables.table_schema,
                        tables.table_name,
                        tables.table_type,
                        tables.table_comment,
                        columns.column_name,
                        columns.column_index,
                        columns.column_type,
                        columns.column_comment

                    from tables
                    join columns
                        on tables."table_database" = columns."table_database"
                        and tables."table_schema" = columns."table_schema"
                        and tables."table_name" = columns."table_name"

                )

                {%- for schema, relations in schemas.items() -%}
                  {%- for relation_batch in relations|batch(100) %}
                    select * from catalog
                    where "table_schema" = lower('{{ schema }}')
                      and (
                        {%- for relation in relation_batch -%}
                          "table_name" = lower('{{ relation }}')
                        {%- if not loop.last %} or {% endif -%}
                        {%- endfor -%}
                      )

                    {%- if not loop.last %} union all {% endif -%}
                  {%- endfor -%}

                  {%- if not loop.last %} union all {% endif -%}
                {%- endfor -%}
            )
        )
  {%- endset -%}

  {{ return(run_query(query)) }}

{%- endmacro %}


{% macro athena__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True) %}
    select
        distinct schema_name

    from {{ information_schema_name(database) }}.schemata
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro athena__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    WITH views AS (
      select
        table_catalog as database,
        table_name as name,
        table_schema as schema
      from {{ schema_relation.information_schema() }}.views
      where table_schema = LOWER('{{ schema_relation.schema }}')
    ), tables AS (
      select
        table_catalog as database,
        table_name as name,
        table_schema as schema

      from {{ schema_relation.information_schema() }}.tables
      where table_schema = LOWER('{{ schema_relation.schema }}')

      -- Views appear in both `tables` and `views`, so excluding them from tables
      EXCEPT

      select * from views
    )
    select views.*, 'view' AS table_type FROM views
    UNION ALL
    select tables.*, 'table' AS table_type FROM tables
  {% endcall %}
  {% do return(load_result('list_relations_without_caching').table) %}
{% endmacro %}

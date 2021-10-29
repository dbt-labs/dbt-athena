{% macro set_table_classification(relation, default_value) -%}
    {%- set format = config.get('format', default=default_value) -%}

    {% call statement('set_table_classification', auto_begin=False) -%}
        alter table {{ relation }} set tblproperties ('classification' = '{{ format }}')
    {%- endcall %}
{%- endmacro %}

{% macro athena__create_table_as(temporary, relation, sql) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set bucketed_by = config.get('bucketed_by', default=none) -%}
  {%- set bucket_count = config.get('bucket_count', default=none) -%}
  {%- set field_delimiter = config.get('field_delimiter', default=none) -%}
  {%- set format = config.get('format', default='parquet') -%}

  create table
    {{ relation }}

    with (
      {%- if external_location is not none and not temporary %}
        external_location='{{ external_location }}',
      {%- endif %}
      {%- if partitioned_by is not none %}
        partitioned_by=ARRAY{{ partitioned_by | tojson | replace('\"', '\'') }},
      {%- endif %}
      {%- if bucketed_by is not none %}
        bucketed_by=ARRAY{{ bucketed_by | tojson | replace('\"', '\'') }},
      {%- endif %}
      {%- if bucket_count is not none %}
        bucket_count={{ bucket_count }},
      {%- endif %}
      {%- if field_delimiter is not none %}
        field_delimiter='{{ field_delimiter }}',
      {%- endif %}
        format='{{ format }}'
    )
  as
    {{ sql }}
{% endmacro %}

{% macro athena__create_view_as(relation, sql) -%}
  create or replace view
    {{ relation }}
  as
    {{ sql }}
{% endmacro %}

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
      where LOWER(table_schema) = LOWER('{{ schema_relation.schema }}')
    ), tables AS (
      select
        table_catalog as database,
        table_name as name,
        table_schema as schema

      from {{ schema_relation.information_schema() }}.tables
      where LOWER(table_schema) = LOWER('{{ schema_relation.schema }}')

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

{% macro athena__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

      select
          column_name,
          data_type,
          null as character_maximum_length,
          null as numeric_precision,
          null as numeric_scale

      from {{ relation.information_schema('columns') }}
      where LOWER(table_name) = LOWER('{{ relation.identifier }}')
        {% if relation.schema %}
            and LOWER(table_schema) = LOWER('{{ relation.schema }}')
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {% do return(sql_convert_columns_in_relation(table)) %}
{% endmacro %}

{% macro athena__drop_relation(relation) -%}
  {%- do adapter.clean_up_table(relation.schema, relation.table) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro athena__current_timestamp() -%}
    -- pyathena converts time zoned timestamps to strings so lets avoid them
    -- now()
    cast(now() as timestamp)
{%- endmacro %}

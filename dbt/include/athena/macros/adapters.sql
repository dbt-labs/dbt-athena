
{% macro ilike(column, value) -%}
	regexp_like({{ column }}, '(?i)\A{{ value }}\Z')
{%- endmacro %}

{% macro set_table_classification(relation) -%}
    {%- set format = config.get('format', default='parquet') -%}

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
      {%- if external_location is not none %}
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
    select
      tables.table_catalog as database,
      tables.table_name as name,
      tables.table_schema as schema,

      case
        when views.table_name is not null
            then 'view'
        when table_type = 'BASE TABLE'
            then 'table'
        else table_type
      end as table_type

    from {{ schema_relation.information_schema() }}.tables
    left join {{ schema_relation.information_schema() }}.views
      on tables.table_catalog = views.table_catalog
      and tables.table_schema = views.table_schema
      and tables.table_name = views.table_name
    where {{ ilike('tables.table_schema', schema_relation.schema) }}
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
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
      where {{ ilike('table_name', relation.identifier) }}
        {% if relation.schema %}
            and {{ ilike('table_schema', relation.schema) }}
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro athena__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro athena__current_timestamp() -%}
    -- pyathena converts time zoned timestamps to strings so lets avoid them
    -- now()
    cast(now() as timestamp)
{%- endmacro %}

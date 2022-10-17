{% macro drop_iceberg(relation) -%}
  drop table if exists {{ relation }}
{% endmacro %}

{% macro create_table_iceberg(relation, tmp_relation, sql) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set staging_location = config.get('staging_location') -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set bucketed_by = config.get('bucketed_by', default=none) -%}
  {%- set bucket_count = config.get('bucket_count', default=none) -%}
  {%- set write_compression = config.get('write_compression', default=none) -%}

  {%- set target_relation = this.incorporate(type='table') -%}

  {% if tmp_relation is not none %}
     {% do adapter.drop_relation(tmp_relation) %}
  {% endif %}

  -- create tmp table

  {% do run_query(create_tmp_table_iceberg(tmp_relation, sql, staging_location)) %}

  {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}

  {% do run_query(create_iceberg_table_definition(target_relation, dest_columns)) %}

  {{ return(incremental_insert(tmp_relation, target_relation)) }}

{% endmacro %}


{% macro create_tmp_table_iceberg(relation, sql, staging_location) -%}
  create table
    {{ relation }}

    with (
        write_compression='snappy',
        format='parquet'
    )
  as
    {{ sql }}
{% endmacro %}

{% macro create_iceberg_table_definition(relation, dest_columns) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set partitioned_by_csv = partitioned_by | join(', ') -%}
  {%- set dest_columns_with_type = [] -%}

  {%- if external_location is none %}
        {%- set default_location = target.s3_staging_dir -%}
        {%- set external_location= default_location + relation.name + '/' -%}
  {%- endif %}

  {%- for col in dest_columns -%}
	{%- if 'varchar' in col.dtype -%}
        {% set dtype = 'string' -%}
    {%- else -%}
        {% set dtype = col.dtype -%}
    {%- endif -%}

  	{% set t = dest_columns_with_type.append(col.name + ' ' + dtype) -%}
  {%- endfor -%}

  {%- set dest_columns_with_type_csv = dest_columns_with_type | join(', ') -%}


  CREATE TABLE {{ relation }} (
    {{ dest_columns_with_type_csv }}
  )
  {%- if partitioned_by is not none %}
  PARTITIONED BY ({{partitioned_by_csv}})
  {%- endif %}
  LOCATION '{{ external_location }}'
  TBLPROPERTIES ( 'table_type' = 'ICEBERG' )

{% endmacro %}


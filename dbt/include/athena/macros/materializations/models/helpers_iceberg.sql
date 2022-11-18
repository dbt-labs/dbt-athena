{% macro drop_iceberg(relation) -%}
  drop table if exists {{ relation }}
{% endmacro %}

{% macro create_table_iceberg(relation, old_relation, tmp_relation, sql) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set staging_location = config.get('staging_location') -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set write_compression = config.get('write_compression', default=none) -%}

  {%- set target_relation = this.incorporate(type='table') -%}

  {% if tmp_relation is not none %}
     {% do adapter.drop_relation(tmp_relation) %}
  {% endif %}

  -- create tmp table
  {% do run_query(create_tmp_table_iceberg(tmp_relation, sql, staging_location)) %}

  -- get columns from tmp table to retrieve metadata
  {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}

  -- drop old relation after tmp table is ready
  {%- if old_relation is not none -%}
  	{% do run_query(drop_iceberg(old_relation)) %}
  {%- endif -%}

  -- create iceberg table
  {% do run_query(create_iceberg_table_definition(target_relation, dest_columns)) %}

  -- return final insert statement
  {{ return(insert_from_sql(target_relation, sql)) }}

{% endmacro %}


{% macro create_tmp_table_iceberg(relation, sql, staging_location) -%}
  create table
    {{ relation }}
    with (
      write_compression='snappy',
      format='parquet'
    )
  as
    select * from (
        {{ sql }}
    )
    limit 0
{% endmacro %}

{% macro create_iceberg_table_definition(relation, dest_columns) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set strict_location = config.get('strict_location', default=true) -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set table_properties = config.get('table_properties', default={}) -%}
  {%- set _ = table_properties.update({'table_type': 'ICEBERG'}) -%}
  {%- set table_properties_formatted = [] -%}
  {%- set dest_columns_with_type = [] -%}

  {%- for k in table_properties -%}
  	{% set _ = table_properties_formatted.append("'" + k + "'='" + table_properties[k] + "'") -%}
  {%- endfor -%}

  {%- set table_properties_csv= table_properties_formatted | join(', ') -%}

  {%- set external_location = adapter.s3_unique_location(external_location, strict_location, target.s3_staging_dir, relation.name) -%}

  {%- for col in dest_columns -%}
	{% set dtype = iceberg_data_type(col.dtype) -%}
  	{% set _ = dest_columns_with_type.append(col.name + ' ' + dtype) -%}
  {%- endfor -%}

  {%- set dest_columns_with_type_csv = dest_columns_with_type | join(', ') -%}

  CREATE TABLE {{ relation }} (
    {{ dest_columns_with_type_csv }}
  )
  {%- if partitioned_by is not none %}
    {%- set partitioned_by_csv = partitioned_by | join(', ') -%}
  	PARTITIONED BY ({{partitioned_by_csv}})
  {%- endif %}
  LOCATION '{{ external_location }}'
  TBLPROPERTIES (
  	{{table_properties_csv}}
  )
{% endmacro %}


{% macro insert_from_sql(target_relation, sql, statement_name="main") %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  insert into {{ target_relation }} ({{ dest_cols_csv }}) (
    select {{ dest_cols_csv }}
    from (
      {{sql}}
      )
    );
{%- endmacro %}


{% macro iceberg_data_type(col_type) -%}
  {%- if 'varchar' in col_type -%}
    {% set data_type = 'string' -%}
  {%- elif 'integer' == col_type -%}
     {% set data_type = 'int' -%}
  {%- else -%}
     {% set data_type = col_type -%}
  {%- endif -%}

  {{ return(data_type) }}
{% endmacro %}

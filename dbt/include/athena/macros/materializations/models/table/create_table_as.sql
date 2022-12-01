{% macro athena__create_table_as(temporary, relation, sql) -%}
  {%- set external_location = config.get('external_location', default=none) -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set bucketed_by = config.get('bucketed_by', default=none) -%}
  {%- set bucket_count = config.get('bucket_count', default=none) -%}
  {%- set field_delimiter = config.get('field_delimiter', default=none) -%}
  {%- set format = config.get('format', default='parquet') -%}
  {%- set write_compression = config.get('write_compression', default=none) -%}
  {%- set s3_data_dir = config.get('s3_data_dir', default=target.s3_data_dir) -%}
  {%- set s3_data_naming = config.get('s3_data_naming', default=target.s3_data_naming) -%}

  {%- if format == 'iceberg' -%}
    {%- set location_property = 'location' -%}
    {%- set partition_property = 'partitioning' -%}
    {%- if bucketed_by is not none or bucket_count is not none -%}
      {%- set error_bucket_iceberg -%}
      bucketed_by or bucket_count cannot be used with Iceberg tables. You have to use the bucket function
      when partitioning
      {%- endset -%}
      {% do exceptions.raise_compiler_error(error_bucket_iceberg) %}
    {%- endif -%}
    {%- if s3_data_naming in ['table', 'table_schema'] -%}
      {%- set error_s3_data_naming_iceberg -%}
        S3 data naming must be set to a strategy that enforces unique table locations when creating Iceberg table
        using CTAS because we are using the RENAME feature to provide near-zero downtime.
      {%- endset -%}
      {% do exceptions.raise_compiler_error(error_s3_data_naming_iceberg) %}
    {%- endif -%}
  {%- else -%}
    {%- set location_property = 'external_location' -%}
    {%- set partition_property = 'partitioned_by' -%}
  {%- endif %}

  create table
    {{ relation }}

    with (
        table_type={%- if format == 'iceberg' -%}'iceberg'{%- else -%}'hive'{%- endif %},
        is_external={%- if format == 'iceberg' -%}false{%- else -%}true{%- endif %},
        {{ location_property }}='{{ adapter.s3_table_location(s3_data_dir, s3_data_naming, relation.schema, relation.identifier, external_location, temporary) }}',
      {%- if partitioned_by is not none %}
        {{ partition_property }}=ARRAY{{ partitioned_by | tojson | replace('\"', '\'') }},
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
      {%- if write_compression is not none %}
        write_compression='{{ write_compression }}',
      {%- endif %}
        format={%- if format == 'iceberg' -%}'parquet'{%- else -%}'{{ format }}'{%- endif %}
    )
  as
    {{ sql }}
{% endmacro %}

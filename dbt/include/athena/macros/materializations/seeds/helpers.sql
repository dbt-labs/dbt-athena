{% macro default__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% set sql = "" %}
    -- No truncate in Athena so always drop CSV table and recreate
    {{ drop_relation(old_relation) }}
    {% set sql = create_csv_table(model, agate_table) %}

    {{ return(sql) }}
{% endmacro %}

{% macro athena__create_csv_table(model, agate_table) %}
  {%- set identifier = model['alias'] -%}

  {%- set lf_tags = config.get('lf_tags', default=none) -%}
  {%- set column_override = config.get('column_types', {}) -%}
  {%- set quote_seed_column = config.get('quote_columns', None) -%}
  {%- set s3_data_dir = config.get('s3_data_dir', default=target.s3_data_dir) -%}
  {%- set s3_data_naming = config.get('s3_data_naming', target.s3_data_naming) -%}
  {%- set external_location = config.get('external_location', default=none) -%}

  {%- set s3_location = adapter.upload_seed_to_s3(
    s3_data_dir,
    s3_data_naming,
    external_location,
    model.schema,
    model.name,
    agate_table,
  ) -%}

  {% set sql %}
    CREATE EXTERNAL TABLE {{ this.render_hive() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {% endif -%}
        {%- endfor -%}
    )
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    WITH SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSS'Z',yyyy-MM-dd'T'HH:mm:ss,yyyy-MM-dd HH:mm:ss")
    LOCATION '{{ s3_location }}'
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ adapter.add_lf_tags_to_table(model.schema, identifier, lf_tags) }}

  {{ return(sql) }}
{% endmacro %}

{% macro athena__load_csv_rows(model, agate_table) %}
    SELECT 1
{% endmacro %}

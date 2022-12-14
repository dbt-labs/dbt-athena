{% macro default__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% set sql = "" %}
    -- No truncate in Athena so always drop CSV table and recreate
    {{ drop_relation(old_relation) }}
    {% set sql = create_csv_table(model, agate_table) %}

    {{ return(sql) }}
{% endmacro %}

{% macro athena__create_csv_table(model, agate_table) %}
  {%- set column_override = config.get('column_types', {}) -%}
  {%- set quote_seed_column = config.get('quote_columns', None) -%}
  {%- set s3_data_dir = config.get('s3_data_dir', default=target.s3_data_dir) -%}
  {%- set s3_data_naming = config.get('s3_data_naming', target.s3_data_naming) -%}
  {%- set external_location = config.get('external_location', default=none) -%}

  {% set sql %}
    create external table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {% endif -%}
        {%- endfor -%}
    )
    stored as parquet
    location '{{ adapter.s3_table_location(s3_data_dir, s3_data_naming, model["schema"], model["alias"], external_location) }}'
    tblproperties ('classification'='parquet')
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}

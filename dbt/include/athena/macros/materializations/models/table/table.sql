{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {%- set format = config.get('format', default='parquet') -%}
  {%- set raw_strategy = config.get('table_strategy') -%}
  {%- if not raw_strategy -%}
    {%- if format == 'iceberg' -%}
      {%- set raw_strategy = 'tmp_parquet' -%}
    {%- else -%}
      {%- set raw_strategy = 'ctas' -%}
    {%- endif -%}
  {%- endif -%}
  {%- set strategy = validate_table_strategy(raw_strategy, format) -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {%- if old_relation is not none -%}
    {%- if format != 'iceberg' -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- endif -%}
  {%- endif -%}

  {%- if format == 'iceberg' -%}
	  {%- set build_sql = create_table_iceberg(target_relation, old_relation, strategy, sql) -%}
  {% else %}
    {% set build_sql = create_table_as(False, target_relation, sql) -%}
  {%- endif -%}

  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  -- drop tmp table in case of iceberg
  {%- if format == 'iceberg' -%}
    {% if strategy == 'tmp_parquet' %}
  	  {% do adapter.drop_relation(tmp_relation) %}
    {% else %}
  	    {% do rename_iceberg(target_relation, bkp_relation) %}
        {% do rename_iceberg(tmp_relation, target_relation) %}
        {% do drop_iceberg(bkp_relation) %}
    {% endif %}
  {%- endif -%}

  -- set table properties
  {%- if format != 'iceberg' -%}
    {{ set_table_classification(target_relation, 'parquet') }}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

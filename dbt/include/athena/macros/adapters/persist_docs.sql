{% macro athena__persist_docs(relation, model, for_relation, for_columns) -%}
  {% set persist_relation_docs = for_relation and config.persist_relation_docs() and model.description %}
  {% set persist_column_docs = for_columns and config.persist_column_docs() and model.columns %}
  {% set skip_archive_table_version = not is_incremental() %}
  {% if persist_relation_docs or persist_column_docs %}
    {% do adapter.persist_docs_to_glue(relation,
                                       model,
                                       persist_relation_docs,
                                       persist_column_docs,
                                       skip_archive_table_version=skip_archive_table_version) %}}
  {% endif %}
{% endmacro %}

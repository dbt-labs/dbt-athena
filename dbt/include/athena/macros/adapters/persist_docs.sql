{% macro athena__persist_docs(relation, model, for_relation=true, for_columns=true) -%}
  {% set persist_relation_docs = for_relation and config.persist_relation_docs() and model.description %}
  {% set persist_column_docs = for_columns and config.persist_column_docs() and model.columns %}
  {% if persist_relation_docs or persist_column_docs %}
    {% do adapter.persist_docs_to_glue(
            relation=relation,
            model=model,
            persist_relation_docs=persist_relation_docs,
            persist_column_docs=persist_column_docs,
            skip_archive_table_version=true
         )
    %}}
  {% endif %}
{% endmacro %}

{% macro athena__persist_docs(relation, model, for_relation, for_columns) -%}
  {% set persist_relation_docs = for_relation and config.persist_relation_docs() and model.description %}
  {% set persist_column_docs = for_columns and config.persist_column_docs() and model.columns %}
  {% if (persist_relation_docs or persist_column_docs) and relation.type != 'view' %}
    {% do adapter.persist_docs_to_glue(relation,
                                       model,
                                       persist_relation_docs,
                                       persist_column_docs) %}}
  {% endif %}
{% endmacro %}

{% macro should_batch_inserts() %}
  {% set config_batch_inserts = config.get('batch_inserts') %}
  {% if config_batch_inserts is none %}
    {% set config_batch_inserts = False %}
  {% endif %}
  {% do return(config_batch_inserts) %}
{% endmacro %}


{% materialization view, adapter='athena' -%}
    {%- set identifier = model['alias'] -%}

    {% set target_relation = this.incorporate(type='view') %}
    {% do persist_docs(target_relation, model) %}

    {{ return(create_or_replace_view(run_outside_transaction_hooks=False)) }}
{%- endmaterialization %}

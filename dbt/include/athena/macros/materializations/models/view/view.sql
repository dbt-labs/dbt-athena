{% materialization view, adapter='athena' -%}
    {% set to_return = create_or_replace_view(run_outside_transaction_hooks=False) %}

    {% set target_relation = this.incorporate(type='view') %}
    {% do persist_docs(target_relation, model) %}

    {% do return(to_return) %}
{%- endmaterialization %}

{% materialization view, adapter='athena' -%}
    {% set to_return = create_or_replace_view(run_outside_transaction_hooks=False) %}

    {% set target_relation = this.incorporate(type='view') %}

    {% do return(to_return) %}
{%- endmaterialization %}

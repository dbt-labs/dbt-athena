{% macro run_hooks(hooks, inside_transaction=True) %}
  {% set re = modules.re %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction) %}
    {% set rendered = render(hook.get('sql')) | trim %}
    {% if (rendered | length) > 0 %}
      {%- if re.match("optimize\W+\w+\W+rewrite data using bin_pack", rendered.lower(), re.MULTILINE) -%}
        {%- do adapter.run_optimize_with_partition_limit_catching(rendered) -%}
      {%- else -%}
        {% call statement(auto_begin=inside_transaction) %}
          {{ rendered }}
        {% endcall %}
      {%- endif -%}
    {% endif %}
  {% endfor %}
{% endmacro %}

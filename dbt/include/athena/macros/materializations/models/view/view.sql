{% materialization view, adapter='athena' -%}
    {%- set identifier = model['alias'] -%}

    {%- set lf_tags_config = config.get('lf_tags_config') -%}
    {%- set lf_inherited_tags = config.get('lf_inherited_tags') -%}
    {%- set lf_grants = config.get('lf_grants') -%}

    {%- set versions_to_keep = config.get('versions_to_keep', default=4) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='view') -%}

    {% set to_return = create_or_replace_view(run_outside_transaction_hooks=False) %}

    {% do adapter.expire_glue_table_versions(target_relation, versions_to_keep, False) %}

    {% if lf_tags_config is not none %}
      {{ adapter.add_lf_tags(target_relation, lf_tags_config, lf_inherited_tags) }}
    {% endif %}

    {% if lf_grants is not none %}
      {{ adapter.apply_lf_grants(target_relation, lf_grants) }}
    {% endif %}

    {% set target_relation = this.incorporate(type='view') %}

    {% do persist_docs(target_relation, model) %}

    {% do return(to_return) %}
{%- endmaterialization %}

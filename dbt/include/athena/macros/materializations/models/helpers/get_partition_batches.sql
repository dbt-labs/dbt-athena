{% macro get_partition_batches(sql, as_subquery=True) -%}
    {%- set partitioned_by = config.get('partitioned_by') -%}
    {%- set athena_partitions_limit = config.get('partitions_limit', 100) | int -%}
    {%- set partitioned_keys = adapter.format_partition_keys(partitioned_by) -%}
    {% do log('PARTITIONED KEYS: ' ~ partitioned_keys) %}

    {% call statement('get_partitions', fetch_result=True) %}
        {%- if as_subquery -%}
            select distinct {{ partitioned_keys }} from ({{ sql }}) order by {{ partitioned_keys }};
        {%- else -%}
            select distinct {{ partitioned_keys }} from {{ sql }} order by {{ partitioned_keys }};
        {%- endif -%}
    {% endcall %}

    {%- set table = load_result('get_partitions').table -%}
    {%- set rows = table.rows -%}
    {%- set partitions = {} -%}
    {% do log('TOTAL PARTITIONS TO PROCESS: ' ~ rows | length) %}
    {%- set partitions_batches = [] -%}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}
        {%- for col in row -%}


            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set comp_func = '=' -%}
            {%- if col is none -%}
                {%- set value = 'null' -%}
                {%- set comp_func = ' is ' -%}
            {%- elif column_type == 'integer' or column_type is none -%}
                {%- set value = col | string -%}
            {%- elif column_type == 'string' -%}
                {%- set value = "'" + col + "'" -%}
            {%- elif column_type == 'date' -%}
                {%- set value = "DATE'" + col | string + "'" -%}
            {%- elif column_type == 'timestamp' -%}
                {%- set value = "TIMESTAMP'" + col | string + "'" -%}
            {%- else -%}
                {%- do exceptions.raise_compiler_error('Need to add support for column type ' + column_type) -%}
            {%- endif -%}
            {%- set partition_key = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
            {%- do single_partition.append(partition_key + comp_func + value) -%}
        {%- endfor -%}

        {%- set single_partition_expression = single_partition | join(' and ') -%}

        {%- set batch_number = (loop.index0 / athena_partitions_limit) | int -%}
        {% if not batch_number in partitions %}
            {% do partitions.update({batch_number: []}) %}
        {% endif %}

        {%- do partitions[batch_number].append('(' + single_partition_expression + ')') -%}
        {%- if partitions[batch_number] | length == athena_partitions_limit or loop.last -%}
            {%- do partitions_batches.append(partitions[batch_number] | join(' or ')) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}

{%- endmacro %}

{% macro get_partition_batches(sql, as_subquery=True) -%}
    {# Retrieve partition configuration and set default partition limit #}
    {%- set partitioned_by = config.get('partitioned_by') -%}
    {%- set athena_partitions_limit = config.get('partitions_limit', 100) | int -%}
    {%- set partitioned_keys = adapter.format_partition_keys(partitioned_by) -%}
    {% do log('PARTITIONED KEYS: ' ~ partitioned_keys) %}

    {# Retrieve distinct partitions from the given SQL #}
    {% call statement('get_partitions', fetch_result=True) %}
        {%- if as_subquery -%}
            select distinct {{ partitioned_keys }} from ({{ sql }}) order by {{ partitioned_keys }};
        {%- else -%}
            select distinct {{ partitioned_keys }} from {{ sql }} order by {{ partitioned_keys }};
        {%- endif -%}
    {% endcall %}

    {# Initialize variables to store partition info #}
    {%- set table = load_result('get_partitions').table -%}
    {%- set rows = table.rows -%}
    {%- set ns = namespace(partitions = [], bucket_conditions = {}, bucket_numbers = [], bucket_column = None, is_bucketed = false) -%}

    {# Process each partition row #}
    {%- for row in rows -%}
        {%- set single_partition = [] -%}
        {# Use Namespace to hold the counter for loop index #}
        {%- set counter = namespace(value=0) -%}
        {# Loop through each column in the row #}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {# Process bucketed columns using the new macro with the index #}
            {%- do process_bucket_column(col, partition_key, table, ns, counter.value) -%}

            {# Logic for non-bucketed columns #}
            {%- set bucket_match = modules.re.search('bucket\((.+?),\s*(\d+)\)', partition_key) -%}
            {%- if not bucket_match -%}
                {# For non-bucketed columns, format partition key and value #}
                {%- set column_type = adapter.convert_type(table, counter.value) -%}
                {%- set value, comp_func = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partitioned_by[counter.value]) -%}
                {%- do single_partition.append(partition_key_formatted + comp_func + value) -%}
            {%- endif -%}
            {# Increment the counter #}
            {%- set counter.value = counter.value + 1 -%}
        {%- endfor -%}

        {# Concatenate conditions for a single partition #}
        {%- set single_partition_expression = single_partition | join(' and ') -%}
        {%- if single_partition_expression not in ns.partitions %}
            {%- do ns.partitions.append(single_partition_expression) -%}
        {%- endif -%}
    {%- endfor -%}

    {# Calculate total batches based on bucketing and partitioning #}
    {%- if ns.is_bucketed -%}
        {%- set total_batches = ns.partitions | length * ns.bucket_numbers | length -%}
    {%- else -%}
        {%- set total_batches = ns.partitions | length -%}
    {%- endif -%}
    {% do log('TOTAL PARTITIONS TO PROCESS: ' ~ total_batches) %}

    {# Determine the number of batches per partition limit #}
    {%- set batches_per_partition_limit = (total_batches // athena_partitions_limit) + (total_batches % athena_partitions_limit > 0) -%}

    {# Create conditions for each batch #}
    {%- set partitions_batches = [] -%}
    {%- for i in range(batches_per_partition_limit) -%}
        {%- set batch_conditions = [] -%}
        {%- if ns.is_bucketed -%}
            {# Combine partition and bucket conditions for each batch #}
            {%- for partition_expression in ns.partitions -%}
                {%- for bucket_num in ns.bucket_numbers -%}
                    {%- set bucket_condition = ns.bucket_column + " IN (" + ns.bucket_conditions[bucket_num] | join(", ") + ")" -%}
                    {%- set combined_condition = "(" + partition_expression + ' and ' + bucket_condition + ")" -%}
                    {%- do batch_conditions.append(combined_condition) -%}
                {%- endfor -%}
            {%- endfor -%}
        {%- else -%}
            {# Extend batch conditions with partitions for non-bucketed columns #}
            {%- do batch_conditions.extend(ns.partitions) -%}
        {%- endif -%}
        {# Calculate batch start and end index and append batch conditions #}
        {%- set start_index = i * athena_partitions_limit -%}
        {%- set end_index = start_index + athena_partitions_limit -%}
        {%- do partitions_batches.append(batch_conditions[start_index:end_index] | join(' or ')) -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}

{%- endmacro %}

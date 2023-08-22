models__test_split_part_sql = """
with data as (

    select * from {{ ref('data_split_part') }}

)

select
    {{ split_part('parts', 'split_on', 1) }} as actual,
    result_1 as expected

from data

union all

select
    {{ split_part('parts', 'split_on', 2) }} as actual,
    result_2 as expected

from data

union all

select
    {{ split_part('parts', 'split_on', 3) }} as actual,
    result_3 as expected

from data
"""

models__test_split_part_yml = """
version: 2
models:
  - name: test_split_part
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""

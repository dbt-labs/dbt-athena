import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__table_base_model = """
{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change=var("on_schema_change_strategy"),
    table_type='hive',
  )
}}

select
    1 as id,
    'test 1' as name
{%- if is_incremental() -%},
    current_date as updated_at
{%- endif -%}
"""


class TestHiveOnSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_on_schema_change.sql": models__table_base_model}

    def test__sync_all_columns(self, project):
        relation_name = "hive_on_schema_change"
        args = ["run", "--select", relation_name, "--vars", '{"on_schema_change_strategy": "sync_all_columns"}']

        model_run_initial = run_dbt(args)
        assert model_run_initial.results[0].status == RunStatus.Success

        model_run_incremental = run_dbt(args)
        assert model_run_incremental.results[0].status == RunStatus.Success

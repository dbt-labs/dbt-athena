import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt, run_dbt_and_capture

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
        return {
            "hive_on_schema_change_sync_all_columns.sql": models__table_base_model,
            "hive_on_schema_change_append_new_columns.sql": models__table_base_model,
            "hive_on_schema_change_ignore.sql": models__table_base_model,
            "hive_on_schema_change_fail.sql": models__table_base_model,
        }

    def _column_names(self, project, relation_name):
        result = project.run_sql(f"show columns from {relation_name}", fetch="all")
        column_names = [row[0].strip() for row in result]
        return column_names

    def test__sync_all_columns(self, project):
        relation_name = "hive_on_schema_change_sync_all_columns"
        args = ["run", "--select", relation_name, "--vars", '{"on_schema_change_strategy": "sync_all_columns"}']

        model_run_initial = run_dbt(args)
        assert model_run_initial.results[0].status == RunStatus.Success

        model_run_incremental = run_dbt(args)
        assert model_run_incremental.results[0].status == RunStatus.Success

        new_column_names = self._column_names(project, relation_name)
        assert new_column_names == ["id", "name", "updated_at"]

    def test__append_new_columns(self, project):
        relation_name = "hive_on_schema_change_append_new_columns"
        args = ["run", "--select", relation_name, "--vars", '{"on_schema_change_strategy": "append_new_columns"}']

        model_run_initial = run_dbt(args)
        assert model_run_initial.results[0].status == RunStatus.Success

        model_run_incremental = run_dbt(args)
        assert model_run_incremental.results[0].status == RunStatus.Success

        new_column_names = self._column_names(project, relation_name)
        assert new_column_names == ["id", "name", "updated_at"]

    def test__ignore(self, project):
        relation_name = "hive_on_schema_change_ignore"
        args = ["run", "--select", relation_name, "--vars", '{"on_schema_change_strategy": "ignore"}']

        model_run_initial = run_dbt(args)
        assert model_run_initial.results[0].status == RunStatus.Success

        model_run_incremental = run_dbt(args)
        assert model_run_incremental.results[0].status == RunStatus.Success

        new_column_names = self._column_names(project, relation_name)
        assert new_column_names == ["id", "name"]

    def test__fail(self, project):
        relation_name = "hive_on_schema_change_fail"
        args = ["run", "--select", relation_name, "--vars", '{"on_schema_change_strategy": "fail"}']

        model_run_initial = run_dbt(args)
        assert model_run_initial.results[0].status == RunStatus.Success

        model_run_incremental, log = run_dbt_and_capture(args, expect_pass=False)
        assert model_run_incremental.results[0].status == RunStatus.Error
        assert "The source and target schemas on this incremental model are out of sync!" in log

        new_column_names = self._column_names(project, relation_name)
        assert new_column_names == ["id", "name"]

import pytest
from tests.functional.adapter.utils.parse_dbt_run_output import (
    extract_create_statement_table_names,
    extract_running_create_statements,
)

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__schema_tmp_sql = """
{{ config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partitioned_by=['date_column'],
        tmp_schema='test_temporary_schema_used_to_hold_tmp_tables'
    )
}}
select
    random() as rnd,
    cast(from_iso8601_date('{{ var('logical_date') }}') as date) as date_column
"""


class TestIncrementalTmpSchema:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema_tmp.sql": models__schema_tmp_sql}

    def test__schema_tmp(self, project, capsys):
        relation_name = "schema_tmp"
        tmp_schema_name = "test_temporary_schema_used_to_hold_tmp_tables"
        create_tmp_schema = f"create schema if not exists `{tmp_schema_name}`"
        drop_tmp_schema = f"drop schema if exists `{tmp_schema_name}` cascade"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        first_model_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                '{"logical_date": "2024-01-01"}',
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )

        first_model_run_result = first_model_run.results[0]

        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 1

        out, _ = capsys.readouterr()
        athena_running_create_statements = extract_running_create_statements(out, relation_name)

        assert len(athena_running_create_statements) == 1

        incremental_model_run_result_table_name = extract_create_statement_table_names(
            athena_running_create_statements[0]
        )[0]

        assert tmp_schema_name not in incremental_model_run_result_table_name

        project.run_sql(create_tmp_schema)

        incremental_model_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                '{"logical_date": "2024-01-02"}',
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )

        incremental_model_run_result = incremental_model_run.results[0]

        assert incremental_model_run_result.status == RunStatus.Success

        records_count_incremental_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_incremental_run == 2

        out, _ = capsys.readouterr()
        athena_running_create_statements = extract_running_create_statements(out, relation_name)

        assert len(athena_running_create_statements) == 1

        incremental_model_run_result_table_name = extract_create_statement_table_names(
            athena_running_create_statements[0]
        )[0]

        assert tmp_schema_name == incremental_model_run_result_table_name.split(".")[1].strip('"')

        project.run_sql(drop_tmp_schema)

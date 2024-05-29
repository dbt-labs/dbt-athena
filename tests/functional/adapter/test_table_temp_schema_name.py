import pytest
import yaml
from tests.functional.adapter.utils.parse_dbt_run_output import (
    extract_create_statement_table_names,
    extract_rename_statement_table_names,
    extract_running_ddl_statements,
)

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__iceberg_table = """
{{ config(
        materialized='table',
        table_type='iceberg',
        temp_schema=var('temp_schema_name'),
    )
}}

select
    {{ var('test_id') }} as id
"""


class TestTableIcebergTableUnique:
    @pytest.fixture(scope="class")
    def models(self):
        return {"models__iceberg_table.sql": models__iceberg_table}

    def test__temp_schema_name_iceberg_table(self, project, capsys):
        relation_name = "models__iceberg_table"
        temp_schema_name = f"{project.test_schema}_tmp"
        drop_temp_schema = f"drop schema if exists `{temp_schema_name}` cascade"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"
        model_run_result_test_id_query = f"select id from {project.test_schema}.{relation_name}"

        vars_dict = {
            "temp_schema_name": temp_schema_name,
            "test_id": 1,
        }

        model_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                yaml.safe_dump(vars_dict),
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )

        model_run_result = model_run.results[0]
        assert model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        athena_running_create_statements = extract_running_ddl_statements(out, relation_name, "create table")
        assert len(athena_running_create_statements) == 1

        incremental_model_run_result_table_name = extract_create_statement_table_names(
            athena_running_create_statements[0]
        )[0]
        assert temp_schema_name not in incremental_model_run_result_table_name

        model_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert model_records_count == 1

        model_test_id_in_table = project.run_sql(model_run_result_test_id_query, fetch="all")[0][0]
        assert model_test_id_in_table == 1

        vars_dict["test_id"] = 2

        model_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                yaml.safe_dump(vars_dict),
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )

        model_run_result = model_run.results[0]
        assert model_run_result.status == RunStatus.Success

        model_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert model_records_count == 1

        model_test_id_in_table = project.run_sql(model_run_result_test_id_query, fetch="all")[0][0]
        assert model_test_id_in_table == 2

        out, _ = capsys.readouterr()
        athena_running_create_statements = extract_running_ddl_statements(out, relation_name, "create table")
        assert len(athena_running_create_statements) == 1

        model_run_2_result_table_name = extract_create_statement_table_names(athena_running_create_statements[0])[0]
        assert temp_schema_name in model_run_2_result_table_name

        athena_running_alter_statements = extract_running_ddl_statements(out, relation_name, "alter table")
        assert len(athena_running_alter_statements) == 1

        athena_running_alter_statement_tables = extract_rename_statement_table_names(athena_running_alter_statements[0])
        athena_running_alter_statement_origin_table = athena_running_alter_statement_tables.get("alter_table_names")[0]
        athena_running_alter_statement_renamed_to_table = athena_running_alter_statement_tables.get(
            "rename_to_table_names"
        )[0]

        assert temp_schema_name in athena_running_alter_statement_origin_table
        assert athena_running_alter_statement_renamed_to_table == f"`{project.test_schema}`.`{relation_name}`"

        project.run_sql(drop_temp_schema)

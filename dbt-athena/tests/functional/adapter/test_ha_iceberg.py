import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__table_iceberg_naming_table_unique = """
{{ config(
        materialized='table',
        table_type='iceberg',
        s3_data_naming='table_unique'
    )
}}

select
    1 as id,
    'test 1' as name
union all
select
    2 as id,
    'test 2' as name
"""

models__table_iceberg_naming_table = """
{{ config(
        materialized='table',
        table_type='iceberg',
        s3_data_naming='table'
    )
}}

select
    1 as id,
    'test 1' as name
union all
select
    2 as id,
    'test 2' as name
"""


class TestTableIcebergTableUnique:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_iceberg_table_unique.sql": models__table_iceberg_naming_table_unique}

    def test__table_creation(self, project, capsys):
        relation_name = "table_iceberg_table_unique"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        fist_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = fist_model_run.results[0]
        assert first_model_run_result.status == RunStatus.Success

        first_models_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert first_models_records_count == 2

        second_model_run = run_dbt(["run", "-d", "--select", relation_name])
        second_model_run_result = second_model_run.results[0]
        assert second_model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        # in case of 2nd run we expect that the target table is renamed to __bkp
        alter_statement = (
            f"alter table `{project.test_schema}`.`{relation_name}` "
            f"rename to `{project.test_schema}`.`{relation_name}__bkp`"
        )
        delete_bkp_table_log = (
            f'Deleted table from glue catalog: "awsdatacatalog"."{project.test_schema}"."{relation_name}__bkp"'
        )
        assert alter_statement in out
        assert delete_bkp_table_log in out

        second_models_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert second_models_records_count == 2


# in case s3_data_naming=table for iceberg a compile error must be raised
# with this test we want to be sure that this type of behavior is not violated
class TestTableIcebergTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_iceberg_table.sql": models__table_iceberg_naming_table}

    def test__table_creation(self, project):
        relation_name = "table_iceberg_table"

        with pytest.raises(Exception):
            run_dbt(["run", "--select", relation_name])

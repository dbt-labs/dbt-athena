import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__table_base_model = """
{{ config(
        materialized='table',
        table_type=var("table_type"),
        s3_data_naming='table_unique'
    )
}}

select
    1 as id,
    'test 1' as name,
    {{ cast_timestamp('current_timestamp') }} as created_at
union all
select
    2 as id,
    'test 2' as name,
    {{ cast_timestamp('current_timestamp') }} as created_at
"""


class TestTableFromHiveToIceberg:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_hive_to_iceberg.sql": models__table_base_model}

    def test__table_creation(self, project):
        relation_name = "table_hive_to_iceberg"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        model_run_hive = run_dbt(["run", "--select", relation_name, "--vars", '{"table_type":"hive"}'])
        model_run_result_hive = model_run_hive.results[0]
        assert model_run_result_hive.status == RunStatus.Success
        models_records_count_hive = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert models_records_count_hive == 2

        model_run_iceberg = run_dbt(["run", "--select", relation_name, "--vars", '{"table_type":"iceberg"}'])
        model_run_result_iceberg = model_run_iceberg.results[0]
        assert model_run_result_iceberg.status == RunStatus.Success
        models_records_count_iceberg = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert models_records_count_iceberg == 2


class TestTableFromIcebergToHive:
    @pytest.fixture(scope="class")
    def models(self):
        return {"table_iceberg_to_hive.sql": models__table_base_model}

    def test__table_creation(self, project):
        relation_name = "table_iceberg_to_hive"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        model_run_iceberg = run_dbt(["run", "--select", relation_name, "--vars", '{"table_type":"iceberg"}'])
        model_run_result_iceberg = model_run_iceberg.results[0]
        assert model_run_result_iceberg.status == RunStatus.Success
        models_records_count_iceberg = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert models_records_count_iceberg == 2

        model_run_hive = run_dbt(["run", "--select", relation_name, "--vars", '{"table_type":"hive"}'])
        model_run_result_hive = model_run_hive.results[0]
        assert model_run_result_hive.status == RunStatus.Success
        models_records_count_hive = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert models_records_count_hive == 2

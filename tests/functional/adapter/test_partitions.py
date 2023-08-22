import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

# this query generates 212 records
test_partitions_model_sql = """
select
    random() as rnd,
    cast(date_column as date) as date_column,
    doy(date_column) as doy
from (
    values (
        sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-07-31'), interval '1' day)
    )
) as t1(date_array)
cross join unnest(date_array) as t2(date_column)
"""


class TestHiveTablePartitions:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+table_type": "hive", "+materialized": "table", "+partitioned_by": ["date_column", "doy"]}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hive_partitions.sql": test_partitions_model_sql,
        }

    def test__check_incremental_run_with_partitions(self, project):
        relation_name = "test_hive_partitions"
        model_run_result_row_count_query = "select count(*) as records from {}.{}".format(
            project.test_schema, relation_name
        )

        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 212


class TestIcebergTablePartitions:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
                "+materialized": "table",
                "+partitioned_by": ["DAY(date_column)", "doy"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_iceberg_partitions.sql": test_partitions_model_sql,
        }

    def test__check_incremental_run_with_partitions(self, project):
        relation_name = "test_iceberg_partitions"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 212


class TestIcebergIncrementalPartitions:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
                "+materialized": "incremental",
                "+incremental_strategy": "merge",
                "+unique_key": "doy",
                "+partitioned_by": ["DAY(date_column)", "doy"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_iceberg_partitions_incremental.sql": test_partitions_model_sql,
        }

    def test__check_incremental_run_with_partitions(self, project):
        """
        Check that the incremental run works with iceberg and partitioned datasets
        """

        relation_name = "test_iceberg_partitions_incremental"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        first_model_run = run_dbt(["run", "--select", relation_name, "--full-refresh"])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 212

        incremental_model_run = run_dbt(["run", "--select", relation_name])

        incremental_model_run_result = incremental_model_run.results[0]

        # check that the model run successfully after incremental run
        assert incremental_model_run_result.status == RunStatus.Success

        incremental_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert incremental_records_count == 212

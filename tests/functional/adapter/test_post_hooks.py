import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

# this query generates 8760 records (separated in 365 partitions in case of partition by day)
test_post_hook_sql = """
SELECT
    ts
    , random(1, 1000) as rnd
    , 'cat' || cast(random(1, 5) as varchar) AS category
FROM
    unnest(
        sequence(
            date('2022-{% if is_incremental() %}06{% else %}01{% endif %}-01'),
            cast('2022-12-31 23:59:00' as timestamp(6)),
            INTERVAL '1' HOUR
        )
    ) AS t(ts)
"""


class TestIncrementalIcebergTablePartitionsWithPostOptimize:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
                "+materialized": "incremental",
                "+incremental_strategy": "merge",
                "+table_properties": {
                    "optimize_rewrite_data_file_threshold": "1",
                    "optimize_rewrite_delete_file_threshold": "1",
                },
                "+unique_key": "ts",
                "+partitioned_by": ["category"],
                "+post_hook": ["optimize {{this.render_pure()}} rewrite data using bin_pack"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_iceberg_incremental_partitions_with_post_optimize.sql": test_post_hook_sql,
        }

    def test__check_post_hook_run_with_partitions_first_run(self, project):
        relation_name = "test_iceberg_incremental_partitions_with_post_optimize"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 8760

        second_model_run = run_dbt(["run", "--select", relation_name])
        second_model_run_result = second_model_run.results[0]

        assert second_model_run_result.status == RunStatus.Success

        records_count_second_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_second_run == 8760

        # Third run to trigger optimize failure because too many partitions
        third_model_run = run_dbt(["run", "--select", relation_name])
        third_model_run_result = third_model_run.results[0]

        assert third_model_run_result.status == RunStatus.Success

        records_count_third_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_third_run == 8760

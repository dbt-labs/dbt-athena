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

test_single_nullable_partition_model_sql = """
with data as (
    select
        random() as col_1,
        row_number() over() as id
    from
        unnest(sequence(1, 200))
)

select
    col_1, id
from data
union all
select random() as col_1, NULL as id
union all
select random() as col_1, NULL as id
"""

test_nullable_partitions_model_sql = """
{{ config(
    materialized='table',
    format='parquet',
    s3_data_naming='table',
    partitioned_by=['id', 'date_column']
) }}

with data as (
    select
    random() as rnd,
    row_number() over() as id,
    cast(date_column as date) as date_column
from (
    values (
        sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-07-31'), interval '1' day)
    )
) as t1(date_array)
cross join unnest(date_array) as t2(date_column)
)

select
    rnd,
    case when id  <= 50 then null else id end as id,
    date_column
from data
union all
select
    random() as rnd,
    NULL as id,
    NULL as date_column
union all
select
    random() as rnd,
    NULL as id,
    cast('2023-09-02' as date) as date_column
union all
select
    random() as rnd,
    40 as id,
    NULL as date_column
"""

test_bucket_partitions_sql = """
with non_random_strings as (
    select
        chr(cast(65 + (row_number() over () % 26) as bigint)) ||
    chr(cast(65 + ((row_number() over () + 1) % 26) as bigint)) ||
    chr(cast(65 + ((row_number() over () + 4) % 26) as bigint)) as non_random_str
    from
        (select 1 union all select 2 union all select 3) as temp_table
)
select
    cast(date_column as date) as date_column,
    doy(date_column) as doy,
    nrnd.non_random_str
from (
    values (
        sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-07-24'), interval '1' day)
    )
) as t1(date_array)
cross join unnest(date_array) as t2(date_column)
join non_random_strings nrnd on true
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


class TestHiveNullValuedPartitions:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "hive",
                "+materialized": "table",
                "+partitioned_by": ["id", "date_column"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_nullable_partitions_model.sql": test_nullable_partitions_model_sql,
        }

    def test__check_run_with_partitions(self, project):
        relation_name = "test_nullable_partitions_model"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"
        model_run_result_null_id_count_query = (
            f"select count(*) as records from {project.test_schema}.{relation_name} where id is null"
        )
        model_run_result_null_date_count_query = (
            f"select count(*) as records from {project.test_schema}.{relation_name} where date_column is null"
        )

        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 215

        null_id_count_first_run = project.run_sql(model_run_result_null_id_count_query, fetch="all")[0][0]

        assert null_id_count_first_run == 52

        null_date_count_first_run = project.run_sql(model_run_result_null_date_count_query, fetch="all")[0][0]

        assert null_date_count_first_run == 2


class TestHiveSingleNullValuedPartition:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "hive",
                "+materialized": "table",
                "+partitioned_by": ["id"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_single_nullable_partition_model.sql": test_single_nullable_partition_model_sql,
        }

    def test__check_run_with_partitions(self, project):
        relation_name = "test_single_nullable_partition_model"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 202


class TestIcebergTablePartitionsBuckets:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
                "+materialized": "table",
                "+partitioned_by": ["DAY(date_column)", "doy", "bucket(non_random_str, 5)"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bucket_partitions.sql": test_bucket_partitions_sql,
        }

    def test__check_run_with_bucket_and_partitions(self, project):
        relation_name = "test_bucket_partitions"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 615


class TestIcebergTableBuckets:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
                "+materialized": "table",
                "+partitioned_by": ["bucket(non_random_str, 5)"],
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bucket_partitions.sql": test_bucket_partitions_sql,
        }

    def test__check_run_with_bucket_in_partitions(self, project):
        relation_name = "test_bucket_partitions"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        first_model_run = run_dbt(["run", "--select", relation_name])
        first_model_run_result = first_model_run.results[0]

        # check that the model run successfully
        assert first_model_run_result.status == RunStatus.Success

        records_count_first_run = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert records_count_first_run == 615

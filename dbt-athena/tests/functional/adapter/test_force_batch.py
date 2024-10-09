import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__force_batch_sql = """
{{ config(
        materialized='table',
        partitioned_by=['date_column'],
        force_batch=true
    )
}}

select
    random() as rnd,
    cast(date_column as date) as date_column
from (
    values (
        sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-07-31'), interval '1' day)
    )
) as t1(date_array)
cross join unnest(date_array) as t2(date_column)
"""

models_append_force_batch_sql = """
{{ config(
        materialized='incremental',
        incremental_strategy='append',
        partitioned_by=['date_column'],
        force_batch=true
    )
}}

select
    random() as rnd,
    cast(date_column as date) as date_column
from (
    values (
        sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-07-31'), interval '1' day)
    )
) as t1(date_array)
cross join unnest(date_array) as t2(date_column)
"""

models_merge_force_batch_sql = """
{{ config(
        table_type='iceberg',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['date_column'],
        partitioned_by=['date_column'],
        force_batch=true
    )
}}
{% if is_incremental() %}
    select
        1 as rnd,
        cast(date_column as date) as date_column
    from (
        values (
            sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-07-31'), interval '1' day)
        )
    ) as t1(date_array)
    cross join unnest(date_array) as t2(date_column)
{% else %}
    select
        2 as rnd,
        cast(date_column as date) as date_column
    from (
        values (
            sequence(from_iso8601_date('2023-01-01'), from_iso8601_date('2023-07-31'), interval '1' day)
        )
    ) as t1(date_array)
    cross join unnest(date_array) as t2(date_column)
{% endif %}
"""


class TestForceBatchInsertParam:
    @pytest.fixture(scope="class")
    def models(self):
        return {"force_batch.sql": models__force_batch_sql}

    def test__force_batch_param(self, project):
        relation_name = "force_batch"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        model_run = run_dbt(["run", "--select", relation_name])
        model_run_result = model_run.results[0]
        assert model_run_result.status == RunStatus.Success

        models_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]

        assert models_records_count == 212


class TestAppendForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"models_append_force_batch.sql": models_append_force_batch_sql}

    def test__append_force_batch_param(self, project):
        relation_name = "models_append_force_batch"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"

        model_run = run_dbt(["run", "--select", relation_name])
        model_run_result = model_run.results[0]
        assert model_run_result.status == RunStatus.Success

        models_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert models_records_count == 212


class TestMergeForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"models_merge_force_batch.sql": models_merge_force_batch_sql}

    def test__merge_force_batch_param(self, project):
        relation_name = "models_merge_force_batch"
        model_run_result_row_count_query = f"select count(*) as records from {project.test_schema}.{relation_name}"
        model_run_result_distinct_query = f"select distinct rnd from {project.test_schema}.{relation_name}"

        model_run = run_dbt(["run", "--select", relation_name])
        model_run_result = model_run.results[0]
        assert model_run_result.status == RunStatus.Success

        model_update = run_dbt(["run", "--select", relation_name])
        model_update_result = model_update.results[0]
        assert model_update_result.status == RunStatus.Success

        models_records_count = project.run_sql(model_run_result_row_count_query, fetch="all")[0][0]
        assert models_records_count == 212

        models_distinct_records = project.run_sql(model_run_result_distinct_query, fetch="all")[0][0]
        assert models_distinct_records == 1

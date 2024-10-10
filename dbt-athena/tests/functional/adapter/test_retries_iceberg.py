"""Test parallel insert into iceberg table."""
import copy
import os

import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import check_relations_equal, run_dbt, run_dbt_and_capture

PARALLELISM = 10

base_dbt_profile = {
    "type": "athena",
    "s3_staging_dir": os.getenv("DBT_TEST_ATHENA_S3_STAGING_DIR"),
    "s3_tmp_table_dir": os.getenv("DBT_TEST_ATHENA_S3_TMP_TABLE_DIR"),
    "schema": os.getenv("DBT_TEST_ATHENA_SCHEMA"),
    "database": os.getenv("DBT_TEST_ATHENA_DATABASE"),
    "region_name": os.getenv("DBT_TEST_ATHENA_REGION_NAME"),
    "threads": PARALLELISM,
    "poll_interval": float(os.getenv("DBT_TEST_ATHENA_POLL_INTERVAL", "1.0")),
    "num_retries": 0,
    "work_group": os.getenv("DBT_TEST_ATHENA_WORK_GROUP"),
    "aws_profile_name": os.getenv("DBT_TEST_ATHENA_AWS_PROFILE_NAME") or None,
}

models__target = """
{{
    config(
        table_type='iceberg',
        materialized='table'
    )
}}

select * from (
    values
    (1, -1)
) as t (id, status)
limit 0

"""

models__source = {
    f"model_{i}.sql": f"""
{{{{
    config(
        table_type='iceberg',
        materialized='table',
        tags=['src'],
        pre_hook='insert into target values ({i}, {i})'
    )
}}}}

select 1 as col
"""
    for i in range(PARALLELISM)
}

seeds__expected_target_init = "id,status"
seeds__expected_target_post = "id,status\n" + "\n".join([f"{i},{i}" for i in range(PARALLELISM)])


class TestIcebergRetriesDisabled:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        profile = copy.deepcopy(base_dbt_profile)
        profile["num_iceberg_retries"] = 0
        return profile

    @pytest.fixture(scope="class")
    def models(self):
        return {**{"target.sql": models__target}, **models__source}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_target_init.csv": seeds__expected_target_init,
            "expected_target_post.csv": seeds__expected_target_post,
        }

    def test__retries_iceberg(self, project):
        """Seed should match the model after run"""

        expected__init_seed_name = "expected_target_init"
        run_dbt(["seed", "--select", expected__init_seed_name, "--full-refresh"])

        relation_name = "target"
        model_run = run_dbt(["run", "--select", relation_name])
        model_run_result = model_run.results[0]
        assert model_run_result.status == RunStatus.Success
        check_relations_equal(project.adapter, [relation_name, expected__init_seed_name])

        expected__post_seed_name = "expected_target_post"
        run_dbt(["seed", "--select", expected__post_seed_name, "--full-refresh"])

        run, log = run_dbt_and_capture(["run", "--select", "tag:src"], expect_pass=False)
        assert any(model_run_result.status == RunStatus.Error for model_run_result in run.results)
        assert "ICEBERG_COMMIT_ERROR" in log


class TestIcebergRetriesEnabled:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        profile = copy.deepcopy(base_dbt_profile)
        # we set the iceberg retries to the same number of parallelism to make sure that the retries are working
        profile["num_iceberg_retries"] = PARALLELISM * 2
        return profile

    @pytest.fixture(scope="class")
    def models(self):
        return {**{"target.sql": models__target}, **models__source}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_target_init.csv": seeds__expected_target_init,
            "expected_target_post.csv": seeds__expected_target_post,
        }

    def test__retries_iceberg(self, project):
        """Seed should match the model after run"""

        expected__init_seed_name = "expected_target_init"
        run_dbt(["seed", "--select", expected__init_seed_name, "--full-refresh"])

        relation_name = "target"
        model_run = run_dbt(["run", "--select", relation_name])
        model_run_result = model_run.results[0]
        assert model_run_result.status == RunStatus.Success
        check_relations_equal(project.adapter, [relation_name, expected__init_seed_name])

        expected__post_seed_name = "expected_target_post"
        run_dbt(["seed", "--select", expected__post_seed_name, "--full-refresh"])

        run = run_dbt(["run", "--select", "tag:src"])
        assert all([model_run_result.status == RunStatus.Success for model_run_result in run.results])
        check_relations_equal(project.adapter, [relation_name, expected__post_seed_name])

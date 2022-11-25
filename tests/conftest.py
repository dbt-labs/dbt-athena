import os

import pytest

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "athena",
        "s3_staging_dir": os.getenv("DBT_TEST_ATHENA_S3_STAGING_DIR"),
        "schema": os.getenv("DBT_TEST_ATHENA_SCHEMA"),
        "database": os.getenv("DBT_TEST_ATHENA_DATABASE"),
        "region_name": os.getenv("DBT_TEST_ATHENA_REGION_NAME"),
        "threads": 1,
        "num_retries": 0,
        "work_group": os.getenv("DBT_TEST_ATHENA_WORK_GROUND"),
        "aws_profile_name": os.getenv("DBT_TEST_ATHENA_AWS_PROFILE_NAME") or None,
    }

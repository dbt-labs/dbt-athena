import os

import pytest

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "athena",
        "s3_staging_dir": os.getenv("DBT_TEST_ATHENA_S3_STAGING_DIR"),
        "s3_tmp_table_dir": os.getenv("DBT_TEST_ATHENA_S3_TMP_TABLE_DIR"),
        "region_name": os.getenv("DBT_TEST_ATHENA_REGION_NAME"),
        "database": os.getenv("DBT_TEST_ATHENA_DATABASE"),
        "schema": os.getenv("DBT_TEST_ATHENA_SCHEMA"),
        "work_group": os.getenv("DBT_TEST_ATHENA_WORK_GROUP"),
        "threads": int(os.getenv("DBT_TEST_ATHENA_THREADS", "1")),
        "poll_interval": float(os.getenv("DBT_TEST_ATHENA_POLL_INTERVAL", "1.0")),
        "num_retries": int(os.getenv("DBT_TEST_ATHENA_NUM_RETRIES", "2")),
        "aws_profile_name": os.getenv("DBT_TEST_ATHENA_AWS_PROFILE_NAME") or None,
        "spark_work_group": os.getenv("DBT_TEST_ATHENA_SPARK_WORK_GROUP"),
    }

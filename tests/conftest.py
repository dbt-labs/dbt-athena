import os
from io import StringIO

import pytest

from dbt.events.base_types import EventLevel
from dbt.events.eventmgr import NoFilter
from dbt.events.functions import EVENT_MANAGER, _get_stdout_config

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


@pytest.fixture(scope="function")
def dbt_error_caplog() -> StringIO:
    return _setup_custom_caplog("dbt_error", EventLevel.ERROR)


@pytest.fixture(scope="function")
def dbt_debug_caplog() -> StringIO:
    return _setup_custom_caplog("dbt_debug", EventLevel.DEBUG)


def _setup_custom_caplog(name: str, level: EventLevel):
    capture_config = _get_stdout_config(level)
    capture_config.name = name
    capture_config.filter = NoFilter
    stringbuf = StringIO()
    capture_config.output_stream = stringbuf
    EVENT_MANAGER.add_logger(capture_config)
    return stringbuf

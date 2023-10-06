import os
from io import StringIO

import pytest

from dbt.events.base_types import EventLevel
from dbt.events.eventmgr import LineFormat, NoFilter
from dbt.events.functions import EVENT_MANAGER, _get_stdout_config

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
        "schema": os.getenv("DBT_TEST_ATHENA_SCHEMA"),
        "database": os.getenv("DBT_TEST_ATHENA_DATABASE"),
        "region_name": os.getenv("DBT_TEST_ATHENA_REGION_NAME"),
        "threads": int(os.getenv("DBT_TEST_ATHENA_THREADS", "1")),
        "poll_interval": float(os.getenv("DBT_TEST_ATHENA_POLL_INTERVAL", "1.0")),
        "num_retries": 0,
        "work_group": os.getenv("DBT_TEST_ATHENA_WORK_GROUP"),
        "aws_profile_name": os.getenv("DBT_TEST_ATHENA_AWS_PROFILE_NAME") or None,
    }


@pytest.fixture(scope="function")
def dbt_error_caplog() -> StringIO:
    return _setup_custom_caplog("dbt_error", EventLevel.ERROR)


@pytest.fixture(scope="function")
def dbt_debug_caplog() -> StringIO:
    return _setup_custom_caplog("dbt_debug", EventLevel.DEBUG)


def _setup_custom_caplog(name: str, level: EventLevel):
    capture_config = _get_stdout_config(
        line_format=LineFormat.PlainText,
        level=level,
        use_colors=False,
        log_cache_events=True,
    )
    capture_config.name = name
    capture_config.filter = NoFilter
    string_buf = StringIO()
    capture_config.output_stream = string_buf
    EVENT_MANAGER.add_logger(capture_config)
    return string_buf

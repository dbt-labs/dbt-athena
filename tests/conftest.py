import os
from io import StringIO
from unittest.mock import MagicMock, patch

import boto3
import pytest

import dbt
from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper
from dbt.events.base_types import EventLevel
from dbt.events.eventmgr import NoFilter
from dbt.events.functions import EVENT_MANAGER, _get_stdout_config

from .unit.constants import (
    ATHENA_WORKGROUP,
    AWS_REGION,
    DATA_CATALOG_NAME,
    DATABASE_NAME,
    S3_STAGING_DIR,
    SPARK_WORKGROUP,
)

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
        "spark_work_group": os.getenv("DBT_TEST_ATHENA_SPARK_WORK_GROUP"),
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


@pytest.fixture(scope="class")
def start_session_response(request):
    return request


@pytest.mark.parametrize(
    "start_session_response",
    [
        ({"SessionId": "test_session_id", "State": "CREATING"}),
        ({"SessionId": "test_session_id", "State": "IDLE"}),
        ({"SessionId": "test_session_id", "State": "TERMINATED"}),
    ],
    indirect=True,
)
@pytest.fixture(scope="class")
def athena_client(start_session_response):
    with patch.object(boto3.session.Session, "client", return_value=MagicMock()) as mock_client:
        mock_client.start_session.return_value = start_session_response
        yield mock_client


@patch.object(dbt.adapters.athena.connections, "AthenaCredentials")
@pytest.fixture(scope="class")
def athena_credentials():
    return AthenaCredentials(
        database=DATA_CATALOG_NAME,
        schema=DATABASE_NAME,
        s3_staging_dir=S3_STAGING_DIR,
        region_name=AWS_REGION,
        work_group=ATHENA_WORKGROUP,
        spark_work_group=SPARK_WORKGROUP,
    )


# @pytest.mark.parametrize(
#     "parsed_model", [({"alias": "test_model", "schema": DATABASE_NAME, "config": {"timeout": 10}})], indirect=True
# )
@pytest.fixture(scope="class")
def athena_python_job_helper(athena_client, athena_credentials):
    parsed_model = {"alias": "test_model", "schema": DATABASE_NAME, "config": {"timeout": 10}}
    with patch.object(AthenaPythonJobHelper, "athena_client", return_value=athena_client):
        assert athena_credentials.spark_work_group == "spark"
        athena_job_helper = AthenaPythonJobHelper(parsed_model, athena_credentials)
        yield athena_job_helper

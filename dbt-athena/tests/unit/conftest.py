from io import StringIO
import os
from unittest.mock import MagicMock, patch

import boto3
import pytest

from dbt_common.events import get_event_manager
from dbt_common.events.base_types import EventLevel
from dbt_common.events.logger import LineFormat, LoggerConfig, NoFilter

from dbt.adapters.athena import connections
from dbt.adapters.athena.connections import AthenaCredentials

from tests.unit.utils import MockAWSService
from tests.unit import constants


@pytest.fixture(scope="class")
def athena_client():
    with patch.object(boto3.session.Session, "client", return_value=MagicMock()) as mock_athena_client:
        return mock_athena_client


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = constants.AWS_REGION


@patch.object(connections, "AthenaCredentials")
@pytest.fixture(scope="class")
def athena_credentials():
    return AthenaCredentials(
        database=constants.DATA_CATALOG_NAME,
        schema=constants.DATABASE_NAME,
        s3_staging_dir=constants.S3_STAGING_DIR,
        region_name=constants.AWS_REGION,
        work_group=constants.ATHENA_WORKGROUP,
        spark_work_group=constants.SPARK_WORKGROUP,
    )


@pytest.fixture()
def mock_aws_service(aws_credentials) -> MockAWSService:
    return MockAWSService()


@pytest.fixture(scope="function")
def dbt_error_caplog() -> StringIO:
    return _setup_custom_caplog("dbt_error", EventLevel.ERROR)


@pytest.fixture(scope="function")
def dbt_debug_caplog() -> StringIO:
    return _setup_custom_caplog("dbt_debug", EventLevel.DEBUG)


def _setup_custom_caplog(name: str, level: EventLevel):
    string_buf = StringIO()
    capture_config = LoggerConfig(
        name=name,
        level=level,
        use_colors=False,
        line_format=LineFormat.PlainText,
        filter=NoFilter,
        output_stream=string_buf,
    )
    event_manager = get_event_manager()
    event_manager.add_logger(capture_config)
    return string_buf

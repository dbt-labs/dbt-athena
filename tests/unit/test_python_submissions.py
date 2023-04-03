from unittest.mock import patch

from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper

from .constants import (
    ATHENA_WORKGROUP,
    AWS_REGION,
    DATA_CATALOG_NAME,
    DATABASE_NAME,
    S3_STAGING_DIR,
    SPARK_WORKGROUP,
)
from .utils import MockAWSService


class TestPythonSubmission:
    mock_aws_service = MockAWSService()
    parsed_model = {"alias": "test_model", "schema": DATABASE_NAME, "config": {"timeout": 10}}
    _athena_job_helper = None
    credentials = AthenaCredentials(
        database=DATA_CATALOG_NAME,
        schema=DATABASE_NAME,
        s3_staging_dir=S3_STAGING_DIR,
        region_name=AWS_REGION,
        work_group=ATHENA_WORKGROUP,
        spark_work_group=SPARK_WORKGROUP,
    )

    @property
    def athena_job_helper(self):
        if self._athena_job_helper is None:
            self._athena_job_helper = AthenaPythonJobHelper(self.parsed_model, self.credentials)
        return self._athena_job_helper

    def test_create_session(self):
        obj = self.athena_job_helper

        # Define the mock return values for the _list_sessions and _start_session methods
        mock_list_sessions = {"SessionId": "session123"}
        mock_start_session = {"SessionId": "session456"}

        # Mock the _list_sessions and _start_session methods using the patch decorator
        with patch.object(obj, "_list_sessions", return_value=mock_list_sessions), patch.object(
            obj, "_start_session", return_value=mock_start_session
        ):
            # Call the session_id property and assert that it returns the expected value
            assert obj.session_id == "session123"

            # Call the _list_sessions and _start_session methods to ensure they were called
            obj._list_sessions.assert_called_once()
            obj._start_session.assert_not_called()  # since _list_sessions return value is not None

        # Call the session_id property again to ensure it still returns the expected value after
        # the mock context is complete
        assert obj.session_id == "session123"

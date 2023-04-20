from unittest.mock import patch

import pytest

from dbt.exceptions import DbtRuntimeError


@pytest.mark.usefixtures("athena_python_job_helper")
class TestPythonSubmission:
    @pytest.mark.parametrize(
        "expected_response",
        [
            (None),
            ("test_session_id"),
            (DbtRuntimeError("Unable to create session: test_session_id. Got status: TERMINATED."),),
        ],
    )
    def test_start_session(self, expected_response, athena_python_job_helper):
        athena_python_job_helper.timeout = 0
        with patch.object(
            athena_python_job_helper,
            "_poll_until_session_creation",
            return_value=expected_response,
        ):
            assert athena_python_job_helper._start_session().get("SessionId") == expected_response
            athena_python_job_helper._start_session.assert_called_once()
            athena_python_job_helper._poll_until_session_creation.assert_called_once()

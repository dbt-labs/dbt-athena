from datetime import datetime
from unittest.mock import patch

import pytest
import pytz

from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper
from dbt.exceptions import DbtRuntimeError

from .constants import DATABASE_NAME


@pytest.mark.usefixtures("athena_credentials", "athena_client")
class TestPythonSubmission:
    """
    A class to test the AthenaPythonJobHelper
    """

    @pytest.fixture()
    def athena_job_helper(self, athena_credentials, athena_client, monkeypatch):
        parsed_model = {"alias": "test_model", "schema": DATABASE_NAME, "config": {"timeout": 10}}
        mock_job_helper = AthenaPythonJobHelper(parsed_model, athena_credentials)
        monkeypatch.setattr(mock_job_helper, "athena_client", athena_client)
        return mock_job_helper

    @pytest.mark.parametrize(
        "session_status_response, expected_response",
        [
            pytest.param(
                {"Status": {"SessionId": "test_session_id", "State": "CREATING"}},
                DbtRuntimeError(
                    """Session <MagicMock name='client.start_session().__getitem__()' id='140219810489792'>
                    did not create within 10 seconds."""
                ),
                marks=pytest.mark.xfail,
            ),
            (
                {"Status": {"SessionId": "test_session_id", "State": "IDLE"}},
                {"SessionId": "test_session_id", "State": "IDLE"},
            ),
            pytest.param(
                {"Status": {"SessionId": "test_session_id", "State": "TERMINATED"}},
                DbtRuntimeError("Unable to create session: test_session_id. Got status: TERMINATED."),
                marks=pytest.mark.xfail,
            ),
        ],
    )
    def test_start_session(self, session_status_response, expected_response, athena_job_helper, athena_client):
        with (
            patch.object(athena_job_helper, "_poll_until_execution_completion", return_value=session_status_response),
            patch.object(athena_client, "get_session_status", return_value=session_status_response),
            patch.object(athena_client, "start_session", return_value=session_status_response.get("Status")),
        ):
            response = athena_job_helper._start_session()
            assert response == expected_response

    @pytest.mark.parametrize(
        "session_status_response, expected_response",
        [
            pytest.param(
                {
                    "NextToken": "test_token",
                    "Sessions": [
                        {
                            "Description": "string",
                            "EngineVersion": {"EffectiveEngineVersion": "string", "SelectedEngineVersion": "string"},
                            "NotebookVersion": "string",
                            "SessionId": "string",
                            "Status": {
                                "EndDateTime": "number",
                                "IdleSinceDateTime": "number",
                                "LastModifiedDateTime": "number",
                                "StartDateTime": "number",
                                "State": "IDLE",
                                "StateChangeReason": "string",
                            },
                        }
                    ],
                },
                {
                    "Description": "string",
                    "EngineVersion": {"EffectiveEngineVersion": "string", "SelectedEngineVersion": "string"},
                    "NotebookVersion": "string",
                    "SessionId": "string",
                    "Status": {
                        "EndDateTime": "number",
                        "IdleSinceDateTime": "number",
                        "LastModifiedDateTime": "number",
                        "StartDateTime": "number",
                        "State": "IDLE",
                        "StateChangeReason": "string",
                    },
                },
            ),
            (
                {
                    "NextToken": "string",
                    "Sessions": [],
                },
                None,
            ),
        ],
    )
    def test_list_sessions(self, session_status_response, expected_response, athena_job_helper, athena_client):
        with (patch.object(athena_client, "list_sessions", return_value=session_status_response),):
            response = athena_job_helper._list_sessions()
            assert response == expected_response

    @pytest.mark.parametrize(
        "parsed_models, expected_timeout",
        [
            ({"alias": "test_model", "schema": DATABASE_NAME, "config": {"timeout": 10}}, 10),
            pytest.param(
                {"alias": "test_model", "schema": "test_database", "config": {"timeout": 0}}, 0, marks=pytest.mark.xfail
            ),
            ({"alias": "test_model", "schema": DATABASE_NAME}, 7200),
        ],
    )
    def test_get_timeout(self, parsed_models, expected_timeout, athena_job_helper, monkeypatch):
        monkeypatch.setattr(athena_job_helper, "parsed_model", parsed_models)
        response = athena_job_helper.get_timeout()
        assert response == expected_timeout

    @pytest.mark.parametrize(
        "session_status_response, expected_response",
        [
            (
                {
                    "SessionId": "string",
                    "Status": {
                        "EndDateTime": "number",
                        "IdleSinceDateTime": "number",
                        "LastModifiedDateTime": "number",
                        "StartDateTime": datetime(2023, 4, 21, 0, tzinfo=pytz.timezone("UTC")),
                        "State": "IDLE",
                        "StateChangeReason": "string",
                    },
                },
                {"State": "TERMINATED"},
            ),
            (
                {
                    "SessionId": "string",
                    "Status": {
                        "EndDateTime": "number",
                        "IdleSinceDateTime": "number",
                        "LastModifiedDateTime": "number",
                        "StartDateTime": datetime(2023, 4, 21, 0, tzinfo=pytz.timezone("UTC")),
                        "State": "TERMINATED",
                        "StateChangeReason": "string",
                    },
                },
                None,
            ),
        ],
    )
    def test_terminate_session(
        self, session_status_response, expected_response, athena_job_helper, athena_client, monkeypatch
    ):
        monkeypatch.setattr(athena_job_helper, "timeout", 10)
        with (
            patch.object(athena_client, "get_session_status", return_value=session_status_response),
            patch.object(athena_client, "terminate_session", return_value=expected_response),
        ):
            terminate_session_response = athena_job_helper._terminate_session()
            assert terminate_session_response == expected_response

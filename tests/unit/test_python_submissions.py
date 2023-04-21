from datetime import datetime
from unittest.mock import patch, Mock

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
        with patch.multiple(
            athena_job_helper,
            _poll_until_session_creation=Mock(return_value=session_status_response),
        ), patch.multiple(
            athena_client,
            get_session_status=Mock(return_value=session_status_response),
            start_session=Mock(return_value=session_status_response.get("Status")),
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
                {},
            ),
        ],
    )
    def test_list_sessions(self, session_status_response, expected_response, athena_job_helper, athena_client):
        with patch.object(athena_client, "list_sessions", return_value=session_status_response):
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
    def test_set_timeout(self, parsed_models, expected_timeout, athena_job_helper, monkeypatch):
        monkeypatch.setattr(athena_job_helper, "parsed_model", parsed_models)
        response = athena_job_helper._set_timeout()
        assert response == expected_timeout

    @pytest.mark.parametrize(
        "parsed_models, expected_polling_interval",
        [
            ({"alias": "test_model", "schema": DATABASE_NAME, "config": {"polling_interval": 10}}, 10),
            pytest.param(
                {"alias": "test_model", "schema": "test_database", "config": {"timeout": 0}}, 0, marks=pytest.mark.xfail
            ),
            ({"alias": "test_model", "schema": DATABASE_NAME}, 5),
        ],
    )
    def test_set_polling_interval(self, parsed_models, expected_polling_interval, athena_job_helper, monkeypatch):
        monkeypatch.setattr(athena_job_helper, "parsed_model", parsed_models)
        response = athena_job_helper._set_polling_interval()
        assert response == expected_polling_interval

    @pytest.mark.parametrize(
        "parsed_models, expected_engine_config",
        [
            (
                {
                    "alias": "test_model",
                    "schema": DATABASE_NAME,
                    "config": {
                        "engine_config": {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 44, "DefaultExecutorDpuSize": 1}
                    },
                },
                {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 44, "DefaultExecutorDpuSize": 1},
            ),
            pytest.param(
                {"alias": "test_model", "schema": "test_database", "config": {"engine_config": 0}},
                0,
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {
                    "alias": "test_wrong_model",
                    "schema": "test_database",
                    "config": {"engine_config": {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2}},
                },
                (),
                marks=pytest.mark.xfail,
            ),
            (
                {"alias": "test_model", "schema": DATABASE_NAME},
                {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1},
            ),
        ],
    )
    def test_set_engine_config(self, parsed_models, expected_engine_config, athena_job_helper, monkeypatch):
        monkeypatch.setattr(athena_job_helper, "parsed_model", parsed_models)
        if parsed_models.get("alias") == "test_wrong_model":
            with pytest.raises(KeyError):
                athena_job_helper._set_engine_config()
        response = athena_job_helper._set_engine_config()
        assert response == expected_engine_config

    @pytest.mark.parametrize(
        "session_status_response, test_session_id, expected_response",
        [
            (
                {
                    "SessionId": "test_session_id",
                    "Status": {
                        "EndDateTime": "number",
                        "IdleSinceDateTime": "number",
                        "LastModifiedDateTime": "number",
                        "StartDateTime": datetime(2023, 4, 21, 0, tzinfo=pytz.timezone("UTC")),
                        "State": "IDLE",
                        "StateChangeReason": "string",
                    },
                },
                "test_session_id",
                None,
            ),
            (
                {
                    "SessionId": "test_session_id_2",
                    "Status": {
                        "EndDateTime": "number",
                        "IdleSinceDateTime": "number",
                        "LastModifiedDateTime": "number",
                        "StartDateTime": datetime(2023, 4, 21, 0, tzinfo=pytz.timezone("UTC")),
                        "State": "TERMINATED",
                        "StateChangeReason": "string",
                    },
                },
                "test_session_id_2",
                None,
            ),
        ],
    )
    def test_terminate_session(
        self, session_status_response, test_session_id, expected_response, athena_job_helper, athena_client, monkeypatch
    ):
        with patch.multiple(
            athena_client,
            get_session_status=Mock(return_value=session_status_response),
            terminate_session=Mock(return_value=expected_response),
        ), patch.multiple(
            athena_job_helper,
            _set_session_id=Mock(return_value=test_session_id),
            _set_timeout=Mock(return_value=10),
        ):
            terminate_session_response = athena_job_helper._terminate_session()
            assert terminate_session_response == expected_response

    def test_poll_session_creation(self):
        pass

    def test_poll_execution(self):
        pass

    def test_submission(self):
        pass

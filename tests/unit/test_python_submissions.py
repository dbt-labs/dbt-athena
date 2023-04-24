from datetime import datetime
from unittest.mock import Mock, patch

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
    def teststart_session(self, session_status_response, expected_response, athena_job_helper, athena_client) -> None:
        """
        Test the start_session method of the AthenaJobHelper class.

        Args:
            session_status_response (dict): A dictionary containing the response from the Athena session
            creation status.
            expected_response (Union[dict, DbtRuntimeError]): The expected response from the start_session method.
            athena_job_helper (AthenaPythonJobHelper): An instance of the AthenaPythonJobHelper class.
            athena_client (botocore.client.BaseClient): An instance of the botocore Athena client.

        Returns:
            None
        """
        with patch.multiple(
            athena_job_helper,
            poll_until_session_creation=Mock(return_value=session_status_response),
        ), patch.multiple(
            athena_client,
            get_session_status=Mock(return_value=session_status_response),
            start_session=Mock(return_value=session_status_response.get("Status")),
        ):
            response = athena_job_helper.start_session()
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
    def test_list_sessions(self, session_status_response, expected_response, athena_job_helper, athena_client) -> None:
        """
        Test the _list_sessions method of the AthenaJobHelper class.

        Args:
            session_status_response (dict): The response object to be returned by the mock Athena client.
            expected_response (dict): The expected output of the _list_sessions method.
            athena_job_helper (AthenaPythonJobHelper): An instance of the AthenaPythonJobHelper class.
            athena_client (Mock): A mock instance of the Athena client.

        Returns:
            None: This function only asserts the output of the _list_sessions method.

        Raises:
            AssertionError: If the output of the _list_sessions method does not match the expected output.
        """
        with patch.object(athena_client, "list_sessions", return_value=session_status_response):
            response = athena_job_helper.list_sessions()
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
    def test_set_timeout(self, parsed_models, expected_timeout, athena_job_helper, monkeypatch) -> None:
        """
        Test case to verify that the `_set_timeout` method of the `AthenaPythonJobHelper` class
        returns the expected timeout value.

        Args:
            parsed_models (dict): A dictionary containing the parsed model configuration.
            expected_timeout (int): The expected timeout value.
            athena_job_helper (AthenaPythonJobHelper): An instance of the `AthenaPythonJobHelper` class.
            monkeypatch: A pytest fixture used to mock and patch objects in the test environment.

        Returns:
            None
        """
        monkeypatch.setattr(athena_job_helper, "parsed_model", parsed_models)
        response = athena_job_helper.set_timeout()
        assert response == expected_timeout

    @pytest.mark.parametrize(
        "parsed_models, expected_polling_interval",
        [
            ({"alias": "test_model", "schema": DATABASE_NAME, "config": {"polling_interval": 10}}, 10),
            pytest.param(
                {"alias": "test_model", "schema": "test_database", "config": {"polling_interval": 0}},
                0,
                marks=pytest.mark.xfail,
            ),
            ({"alias": "test_model", "schema": DATABASE_NAME}, 5),
        ],
    )
    def test_set_polling_interval(
        self, parsed_models, expected_polling_interval, athena_job_helper, monkeypatch
    ) -> None:
        """
        Test method to verify that set_polling_interval() method of AthenaPythonJobHelper
        sets the correct polling interval value based on the parsed model configuration.

        Args:
            parsed_models (dict): Dictionary containing the parsed configuration model for the Athena job.
            expected_polling_interval (int): The expected polling interval value based on the
            parsed model configuration.
            athena_job_helper (AthenaPythonJobHelper): The instance of AthenaPythonJobHelper to be tested.
            monkeypatch: A pytest monkeypatch fixture used to override the parsed model configuration
            in AthenaPythonJobHelper.

        Returns:
            None
        """
        monkeypatch.setattr(athena_job_helper, "parsed_model", parsed_models)
        response = athena_job_helper.set_polling_interval()
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
    def test_set_engine_config(self, parsed_models, expected_engine_config, athena_job_helper, monkeypatch) -> None:
        """
        Test method to verify the `set_engine_config()` method of the AthenaPythonJobHelper class.

        Args:
            parsed_models: A dictionary containing the parsed model configuration data.
            expected_engine_config: The expected engine configuration that is set.
            athena_job_helper: An instance of the AthenaPythonJobHelper class.
            monkeypatch: A fixture from the pytest library that allows modifying attributes at runtime for testing.

        Raises:
            KeyError: If the parsed model configuration dictionary does not contain the required key.

        Returns:
            None. The method asserts that the actual engine configuration set matches the expected engine configuration.
        """
        monkeypatch.setattr(athena_job_helper, "parsed_model", parsed_models)
        if parsed_models.get("alias") == "test_wrong_model":
            with pytest.raises(KeyError):
                athena_job_helper.set_engine_config()
        response = athena_job_helper.set_engine_config()
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
    ) -> None:
        """
        Test function to check if _terminate_session() method of AthenaPythonJobHelper class correctly
        terminates an Athena session.

        Args:
            session_status_response: A mock response object containing the current status of the Athena session.
            test_session_id: The session ID of the test Athena session.
            expected_response: The expected response object after the Athena session is terminated.
            athena_job_helper: An instance of the AthenaPythonJobHelper class.
            athena_client: The mocked Athena client object.
            monkeypatch: Pytest monkeypatch object for patching objects and values during testing.

        Returns:
            None
        """

        with patch.multiple(
            athena_client,
            get_session_status=Mock(return_value=session_status_response),
            terminate_session=Mock(return_value=expected_response),
        ), patch.multiple(
            athena_job_helper,
            set_session_id=Mock(return_value=test_session_id),
            set_timeout=Mock(return_value=10),
        ):
            terminate_session_response = athena_job_helper.terminate_session()
            assert terminate_session_response == expected_response

    def test_poll_session_creation(self):
        pass

    def test_poll_execution(self):
        pass

    def test_submission(self):
        pass

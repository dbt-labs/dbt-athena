from unittest.mock import Mock, patch
from uuid import UUID

import botocore.session
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena import AthenaCredentials
from dbt.adapters.athena.session import AthenaSparkSessionManager, get_boto3_session
from dbt.adapters.contracts.connection import Connection


class TestSession:
    @pytest.mark.parametrize(
        ("credentials_profile_name", "boto_profile_name"),
        (
            pytest.param(None, "default", id="no_profile_in_credentials"),
            pytest.param("my_profile", "my_profile", id="profile_in_credentials"),
        ),
    )
    def test_session_should_be_called_with_correct_parameters(
        self, monkeypatch, credentials_profile_name, boto_profile_name
    ):
        def mock___build_profile_map(_):
            return {**{"default": {}}, **({} if not credentials_profile_name else {credentials_profile_name: {}})}

        monkeypatch.setattr(botocore.session.Session, "_build_profile_map", mock___build_profile_map)
        connection = Connection(
            type="test",
            name="test_session",
            credentials=AthenaCredentials(
                database="db",
                schema="schema",
                s3_staging_dir="dir",
                region_name="eu-west-1",
                aws_profile_name=credentials_profile_name,
            ),
        )
        session = get_boto3_session(connection)
        assert session.region_name == "eu-west-1"
        assert session.profile_name == boto_profile_name


@pytest.mark.usefixtures("athena_credentials", "athena_client")
class TestAthenaSparkSessionManager:
    """
    A class to test the AthenaSparkSessionManager
    """

    @pytest.fixture
    def spark_session_manager(self, athena_credentials, athena_client, monkeypatch):
        """
        Fixture for creating a mock Spark session manager.

        This fixture creates an instance of AthenaSparkSessionManager with the provided Athena credentials,
        timeout, polling interval, and engine configuration. It then patches the Athena client of the manager
        with the provided `athena_client` object. The fixture returns the mock Spark session manager.

        Args:
            self: The test class instance.
            athena_credentials: The Athena credentials.
            athena_client: The Athena client object.
            monkeypatch: The monkeypatch object for mocking.

        Returns:
            The mock Spark session manager.

        """
        mock_session_manager = AthenaSparkSessionManager(
            athena_credentials,
            timeout=10,
            polling_interval=5,
            engine_config={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1},
        )
        monkeypatch.setattr(mock_session_manager, "athena_client", athena_client)
        return mock_session_manager

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
                {"Status": {"SessionId": "635c1c6d-766c-408b-8bce-fae8ea7006f7", "State": "IDLE"}},
                UUID("635c1c6d-766c-408b-8bce-fae8ea7006f7"),
            ),
            pytest.param(
                {"Status": {"SessionId": "test_session_id", "State": "TERMINATED"}},
                DbtRuntimeError("Unable to create session: test_session_id. Got status: TERMINATED."),
                marks=pytest.mark.xfail,
            ),
        ],
    )
    def test_start_session(
        self, session_status_response, expected_response, spark_session_manager, athena_client
    ) -> None:
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
            spark_session_manager,
            poll_until_session_creation=Mock(return_value=session_status_response),
        ), patch.multiple(
            athena_client,
            get_session_status=Mock(return_value=session_status_response),
            start_session=Mock(return_value=session_status_response.get("Status")),
        ):
            response = spark_session_manager.start_session()
            assert response == expected_response

    @pytest.mark.parametrize(
        "session_status_response, expected_status",
        [
            (
                {
                    "SessionId": "test_session_id",
                    "Status": {
                        "State": "CREATING",
                    },
                },
                {
                    "State": "CREATING",
                },
            ),
            (
                {
                    "SessionId": "test_session_id",
                    "Status": {
                        "State": "IDLE",
                    },
                },
                {
                    "State": "IDLE",
                },
            ),
        ],
    )
    def test_get_session_status(self, session_status_response, expected_status, spark_session_manager, athena_client):
        """
        Test the get_session_status function.

        Args:
            self: The test class instance.
            session_status_response (dict): The response from get_session_status.
            expected_status (dict): The expected session status.
            spark_session_manager: The Spark session manager object.
            athena_client: The Athena client object.

        Returns:
            None

        Raises:
            AssertionError: If the retrieved session status is not correct.
        """
        with patch.multiple(athena_client, get_session_status=Mock(return_value=session_status_response)):
            response = spark_session_manager.get_session_status("test_session_id")
            assert response == expected_status

    def test_get_session_id(self):
        pass

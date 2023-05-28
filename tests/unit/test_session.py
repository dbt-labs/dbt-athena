from unittest.mock import Mock, patch
from uuid import UUID

import botocore.session
import pytest

from dbt.adapters.athena import AthenaCredentials, session
from dbt.adapters.athena.session import AthenaSparkSessionManager, get_boto3_session
from dbt.contracts.connection import Connection
from dbt.exceptions import DbtRuntimeError


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
                {
                    "Sessions": [
                        {
                            "SessionId": "635c1c6d-766c-408b-8bce-fae8ea7006f7",
                            "Status": {
                                "StartDateTime": "number",
                                "State": "IDLE",
                            },
                        }
                    ],
                },
                [UUID("635c1c6d-766c-408b-8bce-fae8ea7006f7")],
            ),
            (
                {
                    "Sessions": [],
                },
                [UUID("635c1c6d-766c-408b-8bce-fae8ea7006f7")],
            ),
        ],
    )
    def test_list_sessions(
        self, session_status_response, expected_response, spark_session_manager, athena_client
    ) -> None:
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
        with patch.object(athena_client, "list_sessions", return_value=session_status_response), patch.object(
            AthenaSparkSessionManager, "start_session", return_value=UUID("635c1c6d-766c-408b-8bce-fae8ea7006f7")
        ):
            response = spark_session_manager.list_sessions()
            assert response == expected_response

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

    @pytest.mark.parametrize(
        "list_sessions_response, spark_session_locks, expected_new_sessions",
        [
            (
                [
                    UUID("106d7aca-4b3f-468d-a81d-308120e7f73c"),
                    UUID("39cb8fc0-f855-4b67-91f1-81f068499071"),
                ],
                {"test_session_id": None},
                [
                    UUID("106d7aca-4b3f-468d-a81d-308120e7f73c"),
                    UUID("39cb8fc0-f855-4b67-91f1-81f068499071"),
                ],
            ),
            (
                [],
                {},
                [],
            ),
            (
                [UUID("106d7aca-4b3f-468d-a81d-308120e7f73c")],
                {UUID("106d7aca-4b3f-468d-a81d-308120e7f73c"): "lock"},
                [],
            ),
        ],
    )
    def test_get_sessions(
        self,
        list_sessions_response,
        spark_session_locks,
        expected_new_sessions,
        spark_session_manager,
        athena_client,
        monkeypatch,
    ):
        """
        Test the get_sessions function.

        Args:
            self: The test class instance.
            list_sessions_response (list): The response from list_sessions.
            spark_session_locks (dict): The session locks.
            spark_session_manager: The Spark session manager object.
            athena_client: The Athena client object.
            monkeypatch: The monkeypatch object for mocking.

        Returns:
            None

        Raises:
            AssertionError: If the retrieved sessions are not correct.
        """
        monkeypatch.setattr(session, "spark_session_locks", spark_session_locks)
        with patch.multiple(
            spark_session_manager,
            list_sessions=Mock(return_value=list_sessions_response),
        ):
            sessions = spark_session_manager.get_new_sessions()
            assert sessions == expected_new_sessions

    @pytest.mark.parametrize(
        "get_session_response, current_spark_session_locks",
        [([], {}), ([UUID("106d7aca-4b3f-468d-a81d-308120e7f73c")], {})],
    )
    def test_update_spark_session_locks(
        self, get_session_response, current_spark_session_locks, spark_session_manager, monkeypatch
    ):
        """
        Test the update_spark_session_locks function.

        Args:
            self: The test class instance.
            get_session_response (list): The response from get_sessions.
            current_spark_session_locks (dict): The current session locks.
            spark_session_manager: The Spark session manager object.
            monkeypatch: The monkeypatch object for mocking.

        Raises:
            AssertionError: If the session locks are not updated correctly.
        """
        monkeypatch.setattr(session, "spark_session_locks", current_spark_session_locks)
        with patch.multiple(
            spark_session_manager,
            get_new_sessions=Mock(return_value=get_session_response),
        ):
            spark_session_manager.update_spark_session_locks()
            for each_session in get_session_response:
                assert each_session in session.spark_session_locks.keys()
                assert type(session.spark_session_locks[each_session]) is not None

    def test_get_session_id(self):
        pass

    @pytest.mark.parametrize(
        "test_session_id, get_session_status_response, current_spark_session_locks, terminate_session_response",
        [
            (
                "106d7aca-4b3f-468d-a81d-308120e7f73c",
                {
                    "State": "string",
                },
                {UUID("106d7aca-4b3f-468d-a81d-308120e7f73c"): Mock()},
                {"State": "TERMINATED"},
            ),
            (
                "106d7aca-4b3f-468d-a81d-308120e7f73c",
                {
                    "State": "string",
                },
                {UUID("106d7aca-4b3f-468d-a81d-308120e7f73c"): Mock()},
                {"State": "CREATED"},
            ),
        ],
    )
    def test_release_session_lock(
        self,
        test_session_id,
        get_session_status_response,
        current_spark_session_locks,
        terminate_session_response,
        spark_session_manager,
        athena_client,
        monkeypatch,
    ):
        """
        Test the release_session_lock function.

        Args:
            self: The test class instance.
            test_session_id (str): The ID of the test session.
            get_session_status_response (dict): The response from get_session_status.
            current_spark_session_locks (dict): The current session locks.
            terminate_session_response (dict): The response from terminate_session.
            spark_session_manager: The Spark session manager object.
            athena_client: The Athena client object.
            monkeypatch: The monkeypatch object for mocking.

        Raises:
            AssertionError: If the session lock is not released correctly.
        """
        monkeypatch.setattr(session, "spark_session_locks", current_spark_session_locks)
        with patch.multiple(
            spark_session_manager,
            get_session_status=Mock(return_value=get_session_status_response),
        ), patch.multiple(
            athena_client,
            terminate_session=Mock(return_value=terminate_session_response),
        ):
            spark_session_manager.release_session_lock(test_session_id)
            assert UUID(test_session_id) in session.spark_session_locks.keys()
            assert type(session.spark_session_locks[UUID(test_session_id)]) is not None

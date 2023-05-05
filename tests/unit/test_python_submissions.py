import time
from unittest.mock import Mock, patch

import pytest

from dbt.adapters.athena.python_submissions import (
    AthenaPythonJobHelper,
    AthenaSparkSessionConfig,
    AthenaSparkSessionManager,
)
from dbt.exceptions import DbtRuntimeError

from .constants import DATABASE_NAME


class TestAthenaSparkSessionConfig:
    """
    A class to test AthenaSparkSessionConfig
    """

    @pytest.fixture
    def spark_config(self, request):
        return {
            "timeout": request.param.get("timeout", 7200),
            "polling_interval": request.param.get("polling_interval", 5),
            "engine_config": request.param.get(
                "engine_config", {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}
            ),
        }

    @pytest.fixture
    def spark_config_helper(self, spark_config):
        return AthenaSparkSessionConfig(spark_config)

    @pytest.mark.parametrize(
        "spark_config",
        [
            {"timeout": 5},
            {"timeout": 10},
            {"timeout": 20},
            {},
            pytest.param({"timeout": -1}, marks=pytest.mark.xfail),
            pytest.param({"timeout": None}, marks=pytest.mark.xfail),
        ],
        indirect=True,
    )
    def test_set_timeout(self, spark_config_helper):
        timeout = spark_config_helper.set_timeout()
        assert timeout == spark_config_helper.config.get("timeout", 7200)

    @pytest.mark.parametrize(
        "spark_config",
        [
            {"polling_interval": 5},
            {"polling_interval": 10},
            {"polling_interval": 20},
            {},
            pytest.param({"polling_interval": -1}, marks=pytest.mark.xfail),
        ],
        indirect=True,
    )
    def test_set_polling_interval(self, spark_config_helper):
        polling_interval = spark_config_helper.set_polling_interval()
        assert polling_interval == spark_config_helper.config.get("polling_interval", 5)

    @pytest.mark.parametrize(
        "spark_config",
        [
            {"engine_config": {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}},
            {"engine_config": {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 2}},
            {},
            pytest.param({"engine_config": {"CoordinatorDpuSize": 1}}, marks=pytest.mark.xfail),
            pytest.param({"engine_config": [1, 1, 1]}, marks=pytest.mark.xfail),
        ],
        indirect=True,
    )
    def test_set_engine_config(self, spark_config_helper):
        engine_config = spark_config_helper.set_engine_config()
        assert engine_config == spark_config_helper.config.get(
            "engine_config", {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}
        )


@pytest.mark.usefixtures("athena_credentials", "athena_client")
class TestAthenaSparkSessionManager:
    """
    A class to test the AthenaSparkSessionManager
    """

    @pytest.fixture
    def spark_session_manager(self, athena_credentials, athena_client, monkeypatch):
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
                            "SessionId": "string",
                            "Status": {
                                "StartDateTime": "number",
                                "State": "IDLE",
                            },
                        }
                    ],
                },
                [
                    {
                        "SessionId": "string",
                        "Status": {
                            "StartDateTime": "number",
                            "State": "IDLE",
                        },
                    }
                ],
            ),
            (
                {
                    "Sessions": [],
                },
                {},
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
        with patch.object(athena_client, "list_sessions", return_value=session_status_response):
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
                {"Status": {"SessionId": "test_session_id", "State": "IDLE"}},
                "test_session_id",
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
        with patch.multiple(athena_client, get_session_status=Mock(return_value=session_status_response)):
            response = spark_session_manager.get_session_status("test_session_id")
            assert response == expected_status

    def test_get_sessions(self):
        pass

    def test_update_session_locks(self):
        pass

    def test_get_session_id(self):
        pass

    def test_release_session_lock(self):
        pass


@pytest.mark.usefixtures("athena_credentials", "athena_client")
class TestAthenaPythonJobHelper:
    """
    A class to test the AthenaPythonJobHelper
    """

    @pytest.fixture
    def parsed_model(self, request):
        return {
            "alias": "test_model",
            "schema": DATABASE_NAME,
            "config": {
                "timeout": request.param.get("timeout", 7200),
                "polling_interval": request.param.get("polling_interval", 5),
                "engine_config": request.param.get(
                    "engine_config", {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}
                ),
            },
        }

    @pytest.fixture
    def athena_spark_session_manager(self, athena_credentials, parsed_model):
        return AthenaSparkSessionManager(
            athena_credentials,
            timeout=parsed_model["config"]["timeout"],
            polling_interval=parsed_model["config"]["polling_interval"],
            engine_config=parsed_model["config"]["engine_config"],
        )

    @pytest.fixture
    def athena_job_helper(
        self, athena_credentials, athena_client, athena_spark_session_manager, parsed_model, monkeypatch
    ):
        mock_job_helper = AthenaPythonJobHelper(parsed_model, athena_credentials)
        monkeypatch.setattr(mock_job_helper, "athena_client", athena_client)
        monkeypatch.setattr(mock_job_helper, "spark_connection", athena_spark_session_manager)
        return mock_job_helper

    # @pytest.mark.parametrize(
    #     "session_status_response, test_session_id, expected_response",
    #     [
    #         (
    #             {
    #                 "SessionId": "test_session_id",
    #                 "Status": {
    #                     "EndDateTime": "number",
    #                     "IdleSinceDateTime": "number",
    #                     "LastModifiedDateTime": "number",
    #                     "StartDateTime": datetime(2023, 4, 21, 0, tzinfo=pytz.timezone("UTC")),
    #                     "State": "IDLE",
    #                     "StateChangeReason": "string",
    #                 },
    #             },
    #             "test_session_id",
    #             None,
    #         ),
    #         (
    #             {
    #                 "SessionId": "test_session_id_2",
    #                 "Status": {
    #                     "EndDateTime": "number",
    #                     "IdleSinceDateTime": "number",
    #                     "LastModifiedDateTime": "number",
    #                     "StartDateTime": datetime(2023, 4, 21, 0, tzinfo=pytz.timezone("UTC")),
    #                     "State": "TERMINATED",
    #                     "StateChangeReason": "string",
    #                 },
    #             },
    #             "test_session_id_2",
    #             None,
    #         ),
    #     ],
    # )
    # def test_terminate_session(
    #     self, session_status_response, test_session_id, expected_response, athena_job_helper,
    # athena_client, monkeypatch
    # ) -> None:
    #     """
    #     Test function to check if _terminate_session() method of AthenaPythonJobHelper class correctly
    #     terminates an Athena session.

    #     Args:
    #         session_status_response: A mock response object containing the current status of the Athena session.
    #         test_session_id: The session ID of the test Athena session.
    #         expected_response: The expected response object after the Athena session is terminated.
    #         athena_job_helper: An instance of the AthenaPythonJobHelper class.
    #         athena_client: The mocked Athena client object.
    #         monkeypatch: Pytest monkeypatch object for patching objects and values during testing.

    #     Returns:
    #         None
    #     """

    #     with patch.multiple(
    #         athena_client,
    #         get_session_status=Mock(return_value=session_status_response),
    #         terminate_session=Mock(return_value=expected_response),
    #     ), patch.multiple(
    #         athena_job_helper,
    #         set_session_id=Mock(return_value=test_session_id),
    #         set_timeout=Mock(return_value=10),
    #     ):
    #         terminate_session_response = athena_job_helper.terminate_session()
    #         assert terminate_session_response == expected_response

    @pytest.mark.parametrize(
        "parsed_model, session_status_response, expected_response",
        [
            (
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "IDLE",
                },
                "IDLE",
            ),
            pytest.param(
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "FAILED",
                },
                "FAILED",
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "TERMINATED",
                },
                "TERMINATED",
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "State": "CREATING",
                },
                "CREATING",
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_poll_session_idle(
        self, session_status_response, expected_response, athena_job_helper, athena_spark_session_manager, monkeypatch
    ):
        with patch.multiple(
            athena_spark_session_manager,
            get_session_status=Mock(return_value=session_status_response),
            get_session_id=Mock(return_value="test_session_id"),
        ):

            def mock_sleep(_):
                pass

            monkeypatch.setattr(time, "sleep", mock_sleep)
            poll_response = athena_job_helper.poll_until_session_idle()
            assert poll_response == expected_response

    @pytest.mark.parametrize(
        "parsed_model, execution_status, expected_response",
        [
            (
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "COMPLETED",
                    }
                },
                "COMPLETED",
            ),
            (
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "FAILED",
                    }
                },
                "FAILED",
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "RUNNING",
                    }
                },
                "RUNNING",
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_poll_execution(
        self,
        execution_status,
        expected_response,
        athena_job_helper,
        athena_spark_session_manager,
        athena_client,
        monkeypatch,
    ):
        with patch.multiple(
            athena_spark_session_manager,
            get_session_id=Mock(return_value="test_session_id"),
        ), patch.multiple(
            athena_client,
            get_calculation_execution_status=Mock(return_value=execution_status),
        ):

            def mock_sleep(_):
                pass

            monkeypatch.setattr(time, "sleep", mock_sleep)
            poll_response = athena_job_helper.poll_until_execution_completion("test_calculation_id")
            assert poll_response == expected_response

    @pytest.mark.parametrize(
        "parsed_model, test_calculation_execution_id, test_calculation_execution, test_calculation_execution_status",
        [
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {"CalculationExecutionId": "test_execution_id"},
                {"Result": {"ResultS3Uri": "test_results_s3_uri"}},
                {"Status": {"State": "COMPLETED"}},
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {"CalculationExecutionId": "test_execution_id"},
                {"Result": {"ResultS3Uri": None}},
                {"Status": {"State": "FAILED"}},
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {},
                {"Result": {"ResultS3Uri": None}},
                {"Status": {"State": "FAILED"}},
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_submission(
        self,
        test_calculation_execution_id,
        test_calculation_execution,
        test_calculation_execution_status,
        athena_job_helper,
        athena_spark_session_manager,
        athena_client,
    ):
        with patch.multiple(
            athena_spark_session_manager,
            get_session_id=Mock(return_value="test_session_id"),
            release_session_lock=Mock(),
        ), patch.multiple(
            athena_client,
            start_calculation_execution=Mock(return_value=test_calculation_execution_id),
            get_calculation_execution=Mock(return_value=test_calculation_execution),
            get_calculation_execution_status=Mock(return_value=test_calculation_execution_status),
        ), patch.multiple(
            athena_job_helper, poll_until_session_idle=Mock(return_value="IDLE")
        ):
            result = athena_job_helper.submit("hello world")
            assert result == test_calculation_execution["Result"]["ResultS3Uri"]

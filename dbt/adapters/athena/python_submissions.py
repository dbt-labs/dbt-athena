import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict

import boto3

from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.base import PythonJobHelper
from dbt.events import AdapterLogger
from dbt.exceptions import DbtRuntimeError

DEFAULT_POLLING_INTERVAL = 5
DEFAULT_ENGINE_CONFIG = {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1}
SUBMISSION_LANGUAGE = "python"
DEFAULT_TIMEOUT = 60 * 60 * 2

logger = AdapterLogger("Athena")


class AthenaPythonJobHelper(PythonJobHelper):
    """
     A helper class for executing Python jobs on AWS Athena.

    This class extends the base `PythonJobHelper` class and provides additional functionality
    specific to executing jobs on Athena. It takes a parsed model and credentials as inputs
    during initialization, and provides methods for executing Athena jobs, setting timeout,
    polling interval, region name, AWS profile name, and Spark work group.

    Args:
        parsed_model (Dict): A dictionary representing the parsed model of the Athena job.
            It should contain keys such as 'alias' for job identifier and 'schema' for
            job schema.
        credentials (AthenaCredentials): An instance of the `AthenaCredentials` class
            containing AWS credentials for accessing Athena.

    Attributes:
        identifier (str): A string representing the alias or identifier of the Athena job.
        schema (str): A string representing the schema of the Athena job.
        parsed_model (Dict): A dictionary representing the parsed model of the Athena job.
        timeout (int): An integer representing the timeout value in seconds for the Athena job.
        polling_interval (int): An integer representing the polling interval in seconds for
            checking the status of the Athena job.
        region_name (str): A string representing the AWS region name for executing the Athena job.
        profile_name (str): A string representing the AWS profile name for accessing Athena.
        spark_work_group (str): A string representing the Spark work group for executing the Athena job.

    """

    def __init__(self, parsed_model: Dict, credentials: AthenaCredentials) -> None:
        self.parsed_model = parsed_model
        self.credentials = credentials
        self.athena_client = self.athena_client()

    @property
    def identifier(self) -> str:
        return self.parsed_model["alias"]

    @property
    def schema(self) -> str:
        return self.parsed_model["schema"]

    @property
    @lru_cache()
    def session_id(self) -> str:
        return self._set_session_id()

    def _set_session_id(self) -> str:
        """
        Get the session ID.

        This function retrieves the session ID from the stored session information. If session information
        is not available, a new session is started and its session ID is returned.

        Returns:
            str: The session ID.

        """
        session_info = self._list_sessions()
        if session_info is None:
            return self._start_session().get("SessionId")
        return session_info.get("SessionId")

    @property
    def spark_work_group(self) -> str:
        return self.credentials.spark_work_group

    def athena_client(self) -> Any:
        """
        Get the AWS Athena client.

        This function returns an AWS Athena client object that can be used to interact with the Athena service.
        The client is created using the region name and profile name provided during object instantiation.

        Returns:
            Any: The Athena client object.

        """
        return boto3.session.Session(
            region_name=self.credentials.region_name, profile_name=self.credentials.aws_profile_name
        ).client("athena")

    @property
    def timeout(self) -> int:
        return self._set_timeout()

    def _set_timeout(self) -> int:
        """
        Get the timeout value.

        This function retrieves the timeout value from the parsed model's configuration. If the timeout value
        is not defined, it falls back to the default timeout value. If the retrieved timeout value is less than or
        equal to 0, a ValueError is raised as timeout must be a positive integer.

        Returns:
            int: The timeout value in seconds.

        Raises:
            ValueError: If the timeout value is not a positive integer.

        """
        if self.parsed_model.get("config") is not None:
            timeout = self.parsed_model["config"].get("timeout", DEFAULT_TIMEOUT)
            if not isinstance(timeout, int):
                raise TypeError("Timeout must be an integer")
            if timeout <= 0:
                raise ValueError("Timeout must be a positive integer")
            logger.info(f"Setting timeout: {timeout}")
        else:
            logger.info(f"Using default timeout: {DEFAULT_TIMEOUT}")
            timeout = DEFAULT_TIMEOUT
        return timeout

    @property
    def polling_interval(self):
        return self._set_polling_interval()

    def _set_polling_interval(self) -> int:
        polling_interval = self.parsed_model.get("config", {}).get("polling_interval", DEFAULT_POLLING_INTERVAL)
        if not isinstance(polling_interval, int) or polling_interval <= 0:
            raise ValueError("polling_interval must be a positive integer")
        logger.info(f"Setting polling_interval: {polling_interval}")
        return polling_interval

    @property
    def engine_config(self):
        return self._set_engine_config()

    def _set_engine_config(self) -> dict:
        engine_config = self.parsed_model.get("config", {}).get("engine_config", DEFAULT_ENGINE_CONFIG)
        if not isinstance(engine_config, dict):
            raise TypeError("engine configuration has to be of type dict")

        expected_keys = {"CoordinatorDpuSize", "MaxConcurrentDpus", "DefaultExecutorDpuSize"}
        if set(engine_config.keys()) != expected_keys:
            raise KeyError(f"The keys of the dictionary entered do not match the expected format: {expected_keys}")

        return engine_config

    def _list_sessions(self) -> dict:
        """
        List Athena sessions.

        This function sends a request to the Athena service to list the sessions in the specified Spark workgroup.
        It filters the sessions by state, only returning the first session that is in IDLE state. If no idle sessions
        are found or if an error occurs, None is returned.

        Returns:
            dict: The session information dictionary if an idle session is found, None otherwise.

        """
        response = self.athena_client.list_sessions(WorkGroup=self.spark_work_group, MaxResults=1, StateFilter="IDLE")
        if len(response.get("Sessions")) == 0 or response.get("Sessions") is None:
            return None
        return response.get("Sessions")[0]

    def _start_session(self) -> dict:
        """
        Start an Athena session.

        This function sends a request to the Athena service to start a session in the specified Spark workgroup.
        It configures the session with specific engine configurations. If the session state is not IDLE, the function
        polls until the session creation is complete. The response containing session information is returned.

        Returns:
            dict: The session information dictionary.

        """
        response = self.athena_client.start_session(
            WorkGroup=self.spark_work_group,
            EngineConfiguration=self.engine_config,
        )
        if response["State"] != "IDLE":
            self._poll_until_session_creation(response["SessionId"])
        return response

    def submit(self, compiled_code: str) -> dict:
        """
        Submit a calculation to Athena.

        This function submits a calculation to Athena for execution using the provided compiled code.
        It starts a calculation execution with the current session ID and the compiled code as the code block.
        The function then polls until the calculation execution is completed, and retrieves the result S3 URI.
        If the execution is successful and completed, the result S3 URI is returned. Otherwise, a DbtRuntimeError
        is raised with the execution status.

        Args:
            compiled_code (str): The compiled code to submit for execution.

        Returns:
            dict: The result S3 URI if the execution is successful and completed.

        Raises:
            DbtRuntimeError: If the execution ends in a state other than "COMPLETED".

        """
        try:
            calculation_execution_id = self.athena_client.start_calculation_execution(
                SessionId=self.session_id, CodeBlock=compiled_code.lstrip()
            )["CalculationExecutionId"]
            logger.debug(f"Submitted calculation execution id {calculation_execution_id}")
            execution_status = self._poll_until_execution_completion(calculation_execution_id)
            logger.debug(f"Received execution status {execution_status}")
            if execution_status == "COMPLETED":
                result_s3_uri = self.athena_client.get_calculation_execution(
                    CalculationExecutionId=calculation_execution_id
                )["Result"]["ResultS3Uri"]
                return result_s3_uri
            else:
                raise DbtRuntimeError(f"python model run ended in state {execution_status}")
        finally:
            self._terminate_session()

    def _terminate_session(self) -> None:
        """
        Terminate the current Athena session.

        This function terminates the current Athena session if it is in IDLE or BUSY state and has exceeded the
        configured timeout period. It retrieves the session status, and if the session state is IDLE or BUSY and the
        duration since the session start time exceeds the timeout period, the session is terminated. The session ID is
        used to terminate the session via the Athena client.

        Returns:
            dict: The response from the Athena client after terminating the session.

        """
        session_status = self.athena_client.get_session_status(SessionId=self.session_id)["Status"]
        if session_status["State"] in ["IDLE", "BUSY"] and (
            session_status["StartDateTime"] - datetime.now(tz=timezone.utc) > timedelta(seconds=self.timeout)
        ):
            logger.debug(f"Terminating session: {self.session_id}")
            self.athena_client.terminate_session(SessionId=self.session_id)

    def _poll_until_execution_completion(self, calculation_execution_id):
        """
        Poll the status of a calculation execution until it is completed, failed, or cancelled.

        This function polls the status of a calculation execution identified by the given `calculation_execution_id`
        until it is completed, failed, or cancelled. It uses the Athena client to retrieve the status of the execution
        and checks if the state is one of "COMPLETED", "FAILED", or "CANCELLED". If the execution is not yet completed,
        the function sleeps for a certain polling interval, which starts with the value of `self.polling_interval` and
        doubles after each iteration until it reaches the `self.timeout` period. If the execution does not complete
        within the timeout period, a `DbtRuntimeError` is raised.

        Args:
            calculation_execution_id (str): The ID of the calculation execution to poll.

        Returns:
            str: The final state of the calculation execution, which can be one of "COMPLETED", "FAILED" or "CANCELLED".

        Raises:
            DbtRuntimeError: If the calculation execution does not complete within the timeout period.

        """
        polling_interval = self.polling_interval
        while True:
            execution_status = self.athena_client.get_calculation_execution_status(
                CalculationExecutionId=calculation_execution_id
            )["Status"]["State"]
            if execution_status in ["COMPLETED", "FAILED", "CANCELLED"]:
                return execution_status
            time.sleep(polling_interval)
            polling_interval *= 2
            if polling_interval > self.timeout:
                raise DbtRuntimeError(
                    f"Execution {calculation_execution_id} did not complete within {self.timeout} seconds."
                )

    def _poll_until_session_creation(self, session_id):
        """
        Polls the status of an Athena session creation until it is completed or reaches the timeout.

        Args:
            session_id (str): The ID of the session being created.

        Returns:
            str: The final status of the session, which will be "IDLE" if the session creation is successful.

        Raises:
            DbtRuntimeError: If the session creation fails, is terminated, or degrades during polling.
            DbtRuntimeError: If the session does not become IDLE within the specified timeout.

        """
        polling_interval = self.polling_interval
        while True:
            creation_status = self.athena_client.get_session_status(SessionId=session_id)["Status"]["State"]
            if creation_status in ["FAILED", "TERMINATED", "DEGRADED"]:
                raise DbtRuntimeError(f"Unable to create session: {session_id}. Got status: {creation_status}.")
            elif creation_status == "IDLE":
                return creation_status
            time.sleep(polling_interval)
            polling_interval *= 2
            if polling_interval > self.timeout:
                raise DbtRuntimeError(f"Session {session_id} did not create within {self.timeout} seconds.")

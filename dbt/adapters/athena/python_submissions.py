import time
from functools import cached_property
from typing import Any, Dict

import botocore

from dbt.adapters.athena.config import AthenaSparkSessionConfig
from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.session import AthenaSparkSessionManager
from dbt.adapters.base import PythonJobHelper
from dbt.exceptions import DbtRuntimeError

SUBMISSION_LANGUAGE = "python"


class AthenaPythonJobHelper(PythonJobHelper):
    """
    Default helper to execute python models with Athena Spark.

    Args:
        PythonJobHelper (PythonJobHelper): The base python helper class
    """

    def __init__(self, parsed_model: Dict[Any, Any], credentials: AthenaCredentials) -> None:
        """
        Initialize spark config and connection.

        Args:
            parsed_model (Dict[Any, Any]): The parsed python model.
            credentials (AthenaCredentials): Credentials for Athena connection.
        """
        self.config = AthenaSparkSessionConfig(
            parsed_model.get("config", {}),
            polling_interval=credentials.poll_interval,
            retry_attempts=credentials.num_retries,
        )
        self.spark_connection = AthenaSparkSessionManager(
            credentials, self.timeout, self.polling_interval, self.engine_config
        )

    @cached_property
    def timeout(self) -> int:
        """
        Get the timeout value.

        Returns:
            int: The timeout value in seconds.
        """
        return self.config.set_timeout()

    @cached_property
    def session_id(self) -> str:
        """
        Get the session ID.

        Returns:
            str: The session ID as a string.
        """
        return str(self.spark_connection.get_session_id())

    @cached_property
    def polling_interval(self) -> float:
        """
        Get the polling interval.

        Returns:
            float: The polling interval in seconds.
        """
        return self.config.set_polling_interval()

    @cached_property
    def engine_config(self) -> Dict[str, int]:
        """
        Get the engine configuration.

        Returns:
            Dict[str, int]: A dictionary containing the engine configuration.
        """
        return self.config.set_engine_config()

    @cached_property
    def athena_client(self) -> Any:
        """
        Get the Athena client.

        Returns:
            Any: The Athena client object.
        """
        return self.spark_connection.athena_client

    def get_current_session_status(self) -> Any:
        """
        Get the current session status.

        Returns:
            Any: The status of the session
        """
        return self.spark_connection.get_session_status(self.session_id)

    def poll_until_session_idle(self) -> None:
        """
        Polls the session status until it becomes idle or exceeds the timeout.

        Raises:
            DbtRuntimeError: If the session chosen is not available or if it does not become idle within the timeout.
        """
        polling_interval = self.polling_interval
        while True:
            session_status = self.get_current_session_status()["State"]
            if session_status in ["FAILED", "TERMINATED", "DEGRADED"]:
                raise DbtRuntimeError(f"The session chosen was not available. Got status: {session_status}")
            if session_status == "IDLE":
                break
            time.sleep(polling_interval)
            polling_interval *= 2
            if polling_interval > self.timeout:
                raise DbtRuntimeError(f"Session {self.session_id} did not become free within {self.timeout} seconds.")

    def submit(self, compiled_code: str) -> Any:
        """
        Submit a calculation to Athena.

        This function submits a calculation to Athena for execution using the provided compiled code.
        It starts a calculation execution with the current session ID and the compiled code as the code block.
        The function then polls until the calculation execution is completed, and retrieves the result.
        If the execution is successful and completed, the result S3 URI is returned. Otherwise, a DbtRuntimeError
        is raised with the execution status.

        Args:
            compiled_code (str): The compiled code to submit for execution.

        Returns:
            dict: The result S3 URI if the execution is successful and completed.

        Raises:
            DbtRuntimeError: If the execution ends in a state other than "COMPLETED".

        """
        while True:
            try:
                calculation_execution_id = self.athena_client.start_calculation_execution(
                    SessionId=self.session_id, CodeBlock=compiled_code.lstrip()
                )["CalculationExecutionId"]
                break
            except botocore.exceptions.ClientError as ce:
                LOGGER.exception(f"Encountered client error: {ce}")
                if (
                    ce.response["Error"]["Code"] == "InvalidRequestException"
                    and "Session is in the BUSY state; needs to be IDLE to accept Calculations."
                    in ce.response["Error"]["Message"]
                ):
                    LOGGER.exception("Going to poll until session is IDLE")
                    self.poll_until_session_idle()
            except Exception as e:
                raise DbtRuntimeError(f"Unable to complete python execution. Got: {e}")
        execution_status = self.poll_until_execution_completion(calculation_execution_id)
        LOGGER.debug(f"Received execution status {execution_status}")
        if execution_status == "COMPLETED":
            try:
                result = self.athena_client.get_calculation_execution(CalculationExecutionId=calculation_execution_id)[
                    "Result"
                ]
            except Exception as e:
                LOGGER.error(f"Unable to poll execution status: Got: {e}")
                result = {}
        self.spark_connection.release_session_lock(self.session_id)
        return result

    def poll_until_execution_completion(self, calculation_execution_id: str) -> Any:
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
            if execution_status in ["FAILED", "CANCELLED"]:
                raise DbtRuntimeError(
                    f"""Execution {calculation_execution_id} did not complete successfully.
                    Got: {execution_status} status."""
                )
            if execution_status == "COMPLETED":
                return execution_status
            time.sleep(polling_interval)
            polling_interval *= 2
            if polling_interval > self.timeout:
                raise DbtRuntimeError(
                    f"Execution {calculation_execution_id} did not complete within {self.timeout} seconds."
                )

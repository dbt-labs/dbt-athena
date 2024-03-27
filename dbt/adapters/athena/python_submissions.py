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
        self.relation_name = parsed_model.get("relation_name", None)
        self.config = AthenaSparkSessionConfig(
            parsed_model.get("config", {}),
            polling_interval=credentials.poll_interval,
            retry_attempts=credentials.num_retries,
        )
        self.spark_connection = AthenaSparkSessionManager(
            credentials, self.timeout, self.polling_interval, self.engine_config, self.relation_name
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
        # Seeing an empty calculation along with main python model code calculation is submitted for almost every model
        # Also, if not returning the result json, we are getting green ERROR messages instead of OK messages.
        # And with this handling, the run model code in target folder every model under run folder seems to be empty
        # Need to fix this work around solution
        if compiled_code.strip():
            while True:
                try:
                    LOGGER.debug(
                        f"Model {self.relation_name} - Using session: {self.session_id} to start calculation execution."
                    )
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
                    raise DbtRuntimeError(f"Unable to start spark python code execution. Got: {e}")
            execution_status = self.poll_until_execution_completion(calculation_execution_id)
            LOGGER.debug(f"Model {self.relation_name} - Received execution status {execution_status}")
            if execution_status == "COMPLETED":
                try:
                    result = self.athena_client.get_calculation_execution(
                        CalculationExecutionId=calculation_execution_id
                    )["Result"]
                except Exception as e:
                    LOGGER.error(f"Unable to retrieve results: Got: {e}")
                    result = {}
            return result
        else:
            return {"ResultS3Uri": "string", "ResultType": "string", "StdErrorS3Uri": "string", "StdOutS3Uri": "string"}

    def poll_until_session_idle(self) -> None:
        """
        Polls the session status until it becomes idle or exceeds the timeout.

        Raises:
            DbtRuntimeError: If the session chosen is not available or if it does not become idle within the timeout.
        """
        polling_interval = self.polling_interval
        timer: float = 0
        while True:
            session_status = self.get_current_session_status()["State"]
            if session_status in ["TERMINATING", "TERMINATED", "DEGRADED", "FAILED"]:
                LOGGER.debug(
                    f"Model {self.relation_name} - The session: {self.session_id} was not available. "
                    f"Got status: {session_status}. Will try with a different session."
                )
                self.spark_connection.remove_terminated_session(self.session_id)
                if "session_id" in self.__dict__:
                    del self.__dict__["session_id"]
                break
            if session_status == "IDLE":
                break
            time.sleep(polling_interval)
            timer += polling_interval
            if timer > self.timeout:
                LOGGER.debug(
                    f"Model {self.relation_name} - Session {self.session_id} did not become free within {self.timeout}"
                    " seconds. Will try with a different session."
                )
                if "session_id" in self.__dict__:
                    del self.__dict__["session_id"]
                break

    def poll_until_execution_completion(self, calculation_execution_id: str) -> Any:
        """
        Poll the status of a calculation execution until it is completed, failed, or canceled.

        This function polls the status of a calculation execution identified by the given `calculation_execution_id`
        until it is completed, failed, or canceled. It uses the Athena client to retrieve the status of the execution
        and checks if the state is one of "COMPLETED", "FAILED", or "CANCELED". If the execution is not yet completed,
        the function sleeps for a certain polling interval, which starts with the value of `self.polling_interval` and
        doubles after each iteration until it reaches the `self.timeout` period. If the execution does not complete
        within the timeout period, a `DbtRuntimeError` is raised.

        Args:
            calculation_execution_id (str): The ID of the calculation execution to poll.

        Returns:
            str: The final state of the calculation execution, which can be one of "COMPLETED", "FAILED" or "CANCELED".

        Raises:
            DbtRuntimeError: If the calculation execution does not complete within the timeout period.

        """
        try:
            polling_interval = self.polling_interval
            timer: float = 0
            while True:
                execution_response = self.athena_client.get_calculation_execution(
                    CalculationExecutionId=calculation_execution_id
                )
                execution_session = execution_response.get("SessionId", None)
                execution_status = execution_response.get("Status", None)
                execution_result = execution_response.get("Result", None)
                execution_stderr_s3_path = ""
                if execution_result:
                    execution_stderr_s3_path = execution_result.get("StdErrorS3Uri", None)

                execution_status_state = ""
                execution_status_reason = ""
                if execution_status:
                    execution_status_state = execution_status.get("State", None)
                    execution_status_reason = execution_status.get("StateChangeReason", None)

                if execution_status_state in ["FAILED", "CANCELED"]:
                    raise DbtRuntimeError(
                        f"""Calculation Id:   {calculation_execution_id}
Session Id:     {execution_session}
Status:         {execution_status_state}
Reason:         {execution_status_reason}
Stderr s3 path: {execution_stderr_s3_path}
"""
                    )

                if execution_status_state == "COMPLETED":
                    return execution_status_state

                time.sleep(polling_interval)
                timer += polling_interval
                if timer > self.timeout:
                    self.athena_client.stop_calculation_execution(CalculationExecutionId=calculation_execution_id)
                    raise DbtRuntimeError(
                        f"Execution {calculation_execution_id} did not complete within {self.timeout} seconds."
                    )
        finally:
            self.spark_connection.set_spark_session_load(self.session_id, -1)

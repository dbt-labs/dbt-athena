import time
from functools import cached_property
from io import StringIO
from typing import Any, Dict

import botocore
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.invocation import get_invocation_id

from dbt.adapters.athena.config import (
    AthenaSparkSessionConfig,
    EmrServerlessSparkSessionConfig,
)
from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.session import (
    AthenaSparkSessionManager,
    EmrServerlessSparkSessionManager,
)
from dbt.adapters.base import PythonJobHelper

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
    def engine_config(self) -> Dict[str, str]:
        """
        Get the engine configuration.

        Returns:
            Dict[str, str]: A dictionary containing the engine configuration.
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
                    LOGGER.debug(f"Started execution with calculation Id: {calculation_execution_id}")
                    break
                except botocore.exceptions.ClientError as ce:
                    if (
                        ce.response["Error"]["Code"] == "InvalidRequestException"
                        and "Session is in the BUSY state; needs to be IDLE to accept Calculations."
                        in ce.response["Error"]["Message"]
                    ):
                        LOGGER.warning("Going to poll until session is IDLE")
                        self.poll_until_session_idle()
                    else:
                        raise DbtRuntimeError(f"Encountered client error: {ce}")
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
                        f"""
Calculation Id:  {calculation_execution_id}
Session Id:      {execution_session}
Status:          {execution_status_state}
Reason:          {execution_status_reason}
Stderr s3 path:  {execution_stderr_s3_path}
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


class EmrServerlessJobHelper(PythonJobHelper):
    """
    An implementation of running a PySpark job on EMR Serverless application.
    `run_spark_job` is a synchronous call and waits until the job is in completed state.
    """

    def __init__(self, parsed_model: Dict[Any, Any], credentials: AthenaCredentials) -> None:
        """
        Initialize spark config and connection.

        Args:
            parsed_model (Dict[Any, Any]): The parsed python model.
            credentials (AthenaCredentials): Credentials for Athena connection.
        """
        self.relation_name = parsed_model.get("relation_name", "NA")
        self.config = EmrServerlessSparkSessionConfig(
            parsed_model.get("config", {}),
            polling_interval=credentials.poll_interval,
            retry_attempts=credentials.num_retries,
            s3_staging_dir=credentials.s3_staging_dir,
            emr_job_execution_role_arn=credentials.emr_job_execution_role_arn,
            emr_application_id=credentials.emr_application_id,
            emr_application_name=credentials.emr_application_name,
        )
        self.spark_connection = EmrServerlessSparkSessionManager(credentials)
        self.s3_log_prefix = "logs"
        self.app_type = "SPARK"  # EMR Serverless also supports jobs of type 'HIVE'

    @cached_property
    def timeout(self) -> int:
        """
        Get the timeout value.

        Returns:
            int: The timeout value in minutes.
        """
        return int(self.config.set_timeout() / 60)

    @cached_property
    def polling_interval(self) -> float:
        """
        Get the polling interval.

        Returns:
            float: The polling interval in seconds.
        """
        return self.config.set_polling_interval()

    @cached_property
    def invocation_id(self) -> str:
        """
        Get the dbt invocation unique id

        Returns:
            str: dbt invocation unique id
        """
        return str(get_invocation_id())

    @cached_property
    def emrs_client(self) -> Any:
        """
        Get the EMR Serverless client.

        Returns:
            Any: The EMR Serverless client object.
        """
        return self.spark_connection.emrs_client

    @cached_property
    def s3_client(self) -> Any:
        """
        Get the s3 client.

        Returns:
            Any: The s3 client object.
        """
        return self.spark_connection.s3_client

    @cached_property
    def s3_bucket(self) -> str:
        """
        Get the staging s3 bucket.

        Returns:
            str: The staging s3 bucket.
        """
        return self.config.get_s3_uri().split("/")[2]

    @cached_property
    def job_execution_role_arn(self) -> str:
        """
        Get the emr job execution role arn.

        Returns:
            str: The emr job execution role arn.
        """
        return self.config.get_emr_job_execution_role_arn()

    @cached_property
    def spark_properties(self) -> Dict[str, str]:
        """
        Get the spark properties.

        Returns:
            Dict[str, str]: A dictionary containing the spark properties.
        """
        return self.config.get_spark_properties()

    @cached_property
    def emr_app(self) -> Dict[str, str]:
        """
        Get the emr application based on config and credentials.
        Model configuration value is favored over credential configuration.

        Returns:
            dict: The emr application id or application name
        """
        return self.config.get_emr_application()

    @cached_property
    def application_id(self) -> str:
        """
        Gets the application id based on application name if application id is not configured.

        Returns:
            str: The emr application id
        """
        app = self.emr_app
        if app.get("emr_application_id", None):
            return app["emr_application_id"]
        else:
            app_name = app["emr_application_name"]
            next_token = ""
            args = {
                "maxResults": 1,
                "states": [
                    "CREATING",
                    "CREATED",
                    "STARTING",
                    "STARTED",
                    "STOPPING",
                    "STOPPED",
                ],
            }

            if next_token:
                args["nextToken"] = next_token
            found = False

            while not found:
                try:
                    response = self.emrs_client.list_applications(**args)
                except Exception as e:
                    raise DbtRuntimeError(f"Unable to list emr applications. Got: {e}")
                apps = response.get("applications", None)
                if apps:
                    app_id = next((app["id"] for app in apps if app["name"] == app_name), None)
                if app_id:
                    found = True
                    LOGGER.debug(f"Found emr serverless application id: {app_id}")
                    return str(app_id)
                next_token = response.get("nextToken", None)
                if next_token:
                    args["nextToken"] = next_token
                elif not found:
                    del args["nextToken"]
                    raise DbtRuntimeError(f"No emr serverless application_id found for application name: {app_name}")

        raise DbtRuntimeError(f"No emr serverless application_id found for application name: {app_name}")

    def __str__(self) -> str:
        return f"EMR Serverless {self.app_type} Application: {self.application_id}"

    @cached_property
    def application_ready(self) -> bool:
        """
        Start the emr serverless application - by default and wait until the application is started.

        Returns: True if applicated is started and in ready state
        """
        wait: bool = True
        if self.application_id is None:
            raise DbtRuntimeError(
                "Missing configuration: please configure the emr serverless application_id/application_name."
            )

        try:
            self.emrs_client.start_application(applicationId=self.application_id)
        except Exception as e:
            raise DbtRuntimeError(f"Unable to start emr application: {self.application_id}. Got: {e}")

        app_started = False
        while wait and not app_started:
            try:
                response = self.emrs_client.get_application(applicationId=self.application_id)
            except Exception as e:
                raise DbtRuntimeError(f"Unable to get emr application: {self.application_id}. Got: {e}")
            app_started = response.get("application").get("state") == "STARTED"
            time.sleep(self.polling_interval)
        return app_started

    def save_compiled_code(self, compiled_code: Any) -> str:
        """
        Save the compiled code to the configured staging s3 bucket.

        Returns:
            str: The s3 uri path
        """
        LOGGER.debug(f"Uploading compiled script to {self.s3_bucket}")
        # Create a file-like object from the Python script content
        script_file = StringIO(compiled_code)
        table_name = str(self.relation_name).replace(" ", "_").replace('"', "").replace(".", "_")
        s3_key = f"code/{self.invocation_id}/{table_name}.py"
        try:
            # Upload the Python script content as a file
            self.s3_client.put_object(Body=script_file.getvalue(), Bucket=self.s3_bucket, Key=s3_key)
            LOGGER.debug(f"Python compiled script uploaded to s3://{self.s3_bucket}/{s3_key}")
            return f"s3://{self.s3_bucket}/{s3_key}"
        except Exception as e:
            raise DbtRuntimeError(f"Python compiled script upload to s3://{self.s3_bucket}/{s3_key} failed: {e}")

    def submit(self, compiled_code: str) -> Any:
        """
        Submit a job to EMR serverless when the compiled code script is not blank and the application is started.

        This function submits a job to EMR Serverless for execution using the provided compiled code.
        This saves compiled code into s3 bucket and uses the s3 URI to execute the code.
        The function then polls until the job execution is completed, and retrieves the result.
        If the execution is successful and completed, the result is returned. Otherwise, a DbtRuntimeError
        is raised with the execution status.


        Args:
            compiled_code (str): The compiled code to submit for execution.

        Returns:
            dict: If the execution is successful and completed, returns
            {
            "dbt_invocation_id": str,
            "dbt_model": str,
            "emr_application_id": str,
            "emr_job_run_id": str,
            "script_location": str,
            "stdout_s3_uri": str,
            "stderr_s3_uri": str,
            }

        Raises:
            DbtRuntimeError: If the execution ends in a state other than "COMPLETED".

        """
        if compiled_code.strip() and self.application_ready:
            wait: bool = True
            script_location = self.save_compiled_code(compiled_code)
            try:
                response = self.emrs_client.start_job_run(
                    applicationId=self.application_id,
                    executionRoleArn=self.job_execution_role_arn,
                    executionTimeoutMinutes=self.timeout,
                    jobDriver={"sparkSubmit": {"entryPoint": script_location}},
                    configurationOverrides={
                        "monitoringConfiguration": {
                            "s3MonitoringConfiguration": {"logUri": f"s3://{self.s3_bucket}/{self.s3_log_prefix}"}
                        },
                        "applicationConfiguration": [
                            {
                                "classification": "spark-defaults",
                                "properties": self.spark_properties,
                            }
                        ],
                    },
                )
            except Exception as e:
                raise DbtRuntimeError(
                    f"""Unable to start emr job
dbt invocation id:      {self.invocation_id}
dbt model:              {self.relation_name}
emr application id:     {self.application_id}
job role:               {self.job_execution_role_arn}
script location:        {script_location}
error message:          {e}
"""
                )

            job_run_id = response.get("jobRunId")
            LOGGER.debug(f"Job: {job_run_id} started for model: {self.relation_name}")
            LOGGER.debug(
                f"Job driver log: s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz"
            )

            job_done = False
            while wait and not job_done:
                jr_response = self.get_job_run(job_run_id)
                job_done = jr_response.get("state") in [
                    "SUCCESS",
                    "FAILED",
                    "CANCELLING",
                    "CANCELLED",
                ]
                if jr_response.get("state") == "FAILED":
                    err = jr_response.get("stateDetails")
                    raise DbtRuntimeError(
                        f"""EMR job returned FAILED status:
dbt invocation Id:       {self.invocation_id}
dbt model:               {self.relation_name}
emr application id:      {self.application_id}
emr job run Id:          {job_run_id}
script location:         {script_location}
error message:           {err}
driver log:              s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz
"""
                    )
                elif jr_response.get("state") in ["CANCELLING", "CANCELLED"]:
                    raise DbtRuntimeError(
                        f"""EMR job returned CANCELLED status:
dbt invocation Id:       {self.invocation_id}
dbt model:               {self.relation_name}
emr application id:      {self.application_id}
emr job run Id:          {job_run_id}
script location:         {script_location}
driver log:              s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz
"""
                    )
                elif jr_response.get("state") == "SUCCESS":
                    LOGGER.debug(f"Job: {job_run_id} completed for model: {self.relation_name}")
                    return self.get_driver_log_uri(job_run_id, script_location)

                time.sleep(self.polling_interval)
        else:
            return {"ignore": "empty compiled script"}

    def get_job_run(self, job_run_id: str) -> Any:
        """
        Invokes the emr serverless job run api and returns the response
        """
        try:
            response = self.emrs_client.get_job_run(applicationId=self.application_id, jobRunId=job_run_id)
        except Exception as e:
            raise DbtRuntimeError(
                f"""Unable to get emr job run status
dbt invocation Id:       {self.invocation_id}
dbt model:               {self.relation_name}
emr application id:      {self.application_id}
emr job Id:              {job_run_id}
error message:           {e}
driver log:              s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz
"""
            )
        return response.get("jobRun")

    def get_driver_log_uri(self, job_run_id: str, script_location: str) -> Dict[str, str]:
        """
        Construct a dictionary object with run details
        """
        return {
            "dbt_invocation_id": self.invocation_id,
            "dbt_model": self.relation_name,
            "emr_application_id": self.application_id,
            "emr_job_run_id": job_run_id,
            "script_location": script_location,
            "stdout_s3_uri": f"s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout.gz",
            "stderr_s3_uri": f"s3://{self.s3_bucket}/{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/stderr.gz",
        }

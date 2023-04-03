import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Dict

import boto3

from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.base import PythonJobHelper
from dbt.events import AdapterLogger
from dbt.exceptions import DbtRuntimeError

DEFAULT_POLLING_INTERVAL = 2
SUBMISSION_LANGUAGE = "python"
DEFAULT_TIMEOUT = 60 * 60 * 2

logger = AdapterLogger("Athena")


class AthenaPythonJobHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: AthenaCredentials) -> None:
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.parsed_model = parsed_model
        self.timeout = self.get_timeout()
        self.polling_interval = DEFAULT_POLLING_INTERVAL
        self.region_name = credentials.region_name
        self.profile_name = credentials.aws_profile_name
        self.spark_work_group = credentials.spark_work_group

    @property
    @lru_cache()
    def session_id(self) -> str:
        session_info = self._list_sessions()
        if session_info is None:
            return self._start_session().get("SessionId")
        return session_info.get("SessionId")

    @property
    @lru_cache()
    def athena_client(self) -> Any:
        return boto3.client("athena")

    def get_timeout(self) -> int:
        timeout = self.parsed_model["config"].get("timeout", DEFAULT_TIMEOUT)
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        return timeout

    def _list_sessions(self) -> dict:
        try:
            response = self.athena_client.list_sessions(
                WorkGroup=self.spark_work_group, MaxResults=1, StateFilter="IDLE"
            )
            if len(response.get("Sessions")) == 0 or response.get("Sessions") is None:
                return None
            return response.get("Sessions")[0]
        except Exception:
            raise

    def _start_session(self) -> dict:
        try:
            response = self.athena_client.start_session(
                WorkGroup=self.spark_work_group,
                EngineConfiguration={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1},
            )
            if response["State"] != "IDLE":
                self._poll_until_session_creation(response["SessionId"])
            return response
        except Exception:
            raise

    def submit(self, compiled_code: str) -> dict:
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
        except Exception:
            raise

    def _terminate_session(self) -> dict:
        try:
            session_status = self.athena_client.get_session_status(SessionId=self.session_id)["Status"]
            if session_status["State"] in ["IDLE", "BUSY"] and (
                session_status["StartDateTime"] - datetime.now(tz=timezone.utc) > timedelta(seconds=self.timeout)
            ):
                logger.debug(f"Terminating session: {self.session_id}")
                self.athena_client.terminate_session(SessionId=self.session_id)
        except Exception:
            raise

    def _poll_until_execution_completion(self, calculation_execution_id):
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

    def __del__(self) -> None:
        self._terminate_session()

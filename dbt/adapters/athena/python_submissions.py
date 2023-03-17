from functools import cached_property
from typing import Any, Dict

import boto3

from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.base import PythonJobHelper

DEFAULT_POLLING_INTERVAL = 10
SUBMISSION_LANGUAGE = "python"
DEFAULT_TIMEOUT = 60 * 60 * 24


class AthenaPythonJobHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: AthenaCredentials) -> None:
        self.identifier = parsed_model["alias"]
        self.schema = parsed_model["schema"]
        self.parsed_model = parsed_model
        self.timeout = self.get_timeout()
        self.polling_interval = DEFAULT_POLLING_INTERVAL
        self.region_name = credentials.get_region_name()
        self.profile_name = credentials.get_profile_name()
        self.spark_work_group = credentials.get_spark_work_group()

    @cached_property
    def session_id(self) -> str:
        if self._list_sessions() is None:
            return self._start_session().get("SessionId")
        return self._list_sessions().get("SessionId")

    @cached_property
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
            return response.get("Sessions")[0]
        except Exception:
            return None

    def _start_session(self) -> dict:
        try:
            response = self.athena_client.start_session(
                WorkGroup=self.spark_work_group,
                EngineConfiguration={"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 1, "DefaultExecutorDpuSize": 18},
            )
            return response
        except Exception:
            return None

    def submit(self, compiled_code: str) -> dict:
        try:
            response = self.athena_client.start_calculation_execution(
                SessionId=self.session_id, CodeBlock=compiled_code.strip()
            )
            return response
        except Exception:
            return None

    def _terminate_session(self) -> dict:
        try:
            response = self.athena_client.terminate_session(SessionId=self.session_id)
            return response
        except Exception:
            return None

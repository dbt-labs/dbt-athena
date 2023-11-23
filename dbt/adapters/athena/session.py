import threading
import time
from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Any, Dict, List
from uuid import UUID

import boto3
import boto3.session

from dbt.adapters.athena.config import get_boto3_config
from dbt.adapters.athena.constants import DEFAULT_THREAD_COUNT, LOGGER
from dbt.contracts.connection import Connection
from dbt.exceptions import DbtRuntimeError

spark_session_locks: Dict[UUID, threading.Lock] = {}


def get_boto3_session(connection: Connection) -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=connection.credentials.aws_access_key_id,
        aws_secret_access_key=connection.credentials.aws_secret_access_key,
        aws_session_token=connection.credentials.aws_session_token,
        region_name=connection.credentials.region_name,
        profile_name=connection.credentials.aws_profile_name,
    )


def get_boto3_session_from_credentials(credentials: Any) -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=credentials.aws_access_key_id,
        aws_secret_access_key=credentials.aws_secret_access_key,
        aws_session_token=credentials.aws_session_token,
        region_name=credentials.region_name,
        profile_name=credentials.aws_profile_name,
    )


class AthenaSparkSessionManager:
    """
    A helper class to manage Athena Spark Sessions.
    """

    def __init__(self, credentials: Any, timeout: int, polling_interval: float, engine_config: Dict[str, int]) -> None:
        """
        Initialize the AthenaSparkSessionManager instance.

        Args:
            credentials (Any): The credentials to be used.
            timeout (int): The timeout value in seconds.
            polling_interval (float): The polling interval in seconds.
            engine_config (Dict[str, int]): The engine configuration.

        """
        self.credentials = credentials
        self.timeout = timeout
        self.polling_interval = polling_interval
        self.engine_config = engine_config
        self.lock = threading.Lock()

    @cached_property
    def spark_threads(self) -> int:
        """
        Get the number of Spark threads.

        Returns:
            int: The number of Spark threads. If not found in the profile, returns the default thread count.
        """
        if not self.credentials.spark_threads:
            LOGGER.debug(
                f"""Spark threads not found in profile. Got: {self.credentials.spark_threads}.
                Using default count: {DEFAULT_THREAD_COUNT}"""
            )
            return DEFAULT_THREAD_COUNT
        return int(self.credentials.spark_threads)

    @cached_property
    def spark_work_group(self) -> str:
        """
        Get the Spark work group.

        Returns:
            str: The Spark work group. Raises an exception if not found in the profile.
        """
        if not self.credentials.spark_work_group:
            raise DbtRuntimeError(f"Expected spark_work_group in profile. Got: {self.credentials.spark_work_group}")
        return str(self.credentials.spark_work_group)

    @cached_property
    def athena_client(self) -> Any:
        """
        Get the AWS Athena client.

        This function returns an AWS Athena client object that can be used to interact with the Athena service.
        The client is created using the region name and profile name provided during object instantiation.

        Returns:
            Any: The Athena client object.

        """
        return get_boto3_session_from_credentials(self.credentials).client(
            "athena", config=get_boto3_config(num_retries=self.credentials.effective_num_retries)
        )

    def get_new_sessions(self) -> List[UUID]:
        """
        Retrieves a list of new sessions by subtracting the existing sessions from the complete session list.
        If no new sessions are found a new session is created provided the number of sessions is less than the
        number of allowed spark threads.

        Returns:
            List[UUID]: A list of new session UUIDs.

        """
        sessions = self.list_sessions()
        existing_sessions = set(spark_session_locks.keys())
        new_sessions = [session for session in sessions if session not in existing_sessions]

        if len(new_sessions) == 0:
            if len(spark_session_locks) < self.spark_threads:
                return [self.start_session()]
            LOGGER.warning(
                f"""Maximum spark session count: {self.spark_threads} reached.
            Cannot start new spark session."""
            )
        LOGGER.debug(f"Setting sessions: {new_sessions}")
        return new_sessions

    def update_spark_session_locks(self) -> None:
        """
        Update session locks for each session.

        This function iterates over the existing sessions and ensures that a session lock is created for each session.
        If a session lock already exists, it is left unchanged.

        Args:
            self: The instance of the class.

        Returns:
            None
        """
        for session_uuid in self.get_new_sessions():
            spark_session_locks.setdefault(session_uuid, threading.Lock())
        LOGGER.debug(f"Updated session locks: {spark_session_locks}")

    def get_session_id(self) -> UUID:
        """
        Get a session ID for the Spark session.

        Returns:
            UUID: The session ID.

        Notes:
            This method acquires a lock on an existing available Spark session. If no session is available,
            it waits for a certain period and retries until a session becomes available or a timeout is reached.
            The session ID of the acquired session is returned.

        """
        polling_interval = self.polling_interval
        self.update_spark_session_locks()
        while True:
            for session_uuid, lock in spark_session_locks.items():
                if not lock.locked():
                    LOGGER.debug(f"Locking existing session: {session_uuid}")
                    lock.acquire(blocking=False)
                    return session_uuid
            LOGGER.debug(
                f"""All available spark sessions: {spark_session_locks.keys()} are locked.
                Going to sleep: {polling_interval} seconds."""
            )
            time.sleep(polling_interval)
            polling_interval *= 2

    def list_sessions(self, state: str = "IDLE") -> List[UUID]:
        """
        List the sessions based on the specified state.

        Args:
            state (str, optional): The state to filter the sessions. Defaults to "IDLE".

        Returns:
            List[UUID]: A list of session IDs matching the specified state.

        Notes:
            This method utilizes the Athena client to list sessions in the Spark work group.
            The sessions are filtered based on the provided state.
            If no sessions are found or the response does not contain any sessions, an empty list is returned.

        """
        response = self.athena_client.list_sessions(
            WorkGroup=self.spark_work_group,
            MaxResults=self.spark_threads,
            StateFilter=state,
        )
        if response.get("Sessions") is None:
            return []
        return [UUID(session_string["SessionId"]) for session_string in response.get("Sessions")]

    def start_session(self) -> UUID:
        """
        Start an Athena session.

        This function sends a request to the Athena service to start a session in the specified Spark workgroup.
        It configures the session with specific engine configurations. If the session state is not IDLE, the function
        polls until the session creation is complete. The response containing session information is returned.

        Returns:
            dict: The session information dictionary.

        """
        response = self.athena_client.start_session(
            WorkGroup=self.credentials.spark_work_group,
            EngineConfiguration=self.engine_config,
        )
        if response["State"] != "IDLE":
            self.poll_until_session_creation(response["SessionId"])
        return UUID(response["SessionId"])

    def poll_until_session_creation(self, session_id: str) -> None:
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
            creation_status = self.get_session_status(session_id)["State"]
            if creation_status in ["FAILED", "TERMINATED", "DEGRADED"]:
                raise DbtRuntimeError(f"Unable to create session: {session_id}. Got status: {creation_status}.")
            elif creation_status == "IDLE":
                LOGGER.debug(f"Session: {session_id} created")
                break
            time.sleep(polling_interval)
            polling_interval *= 2
            if polling_interval > self.timeout:
                raise DbtRuntimeError(f"Session {session_id} did not create within {self.timeout} seconds.")

    def release_session_lock(self, session_id: str) -> None:
        """
        Terminate the current Athena session.

        This function terminates the current Athena session if it is in IDLE or BUSY state and has exceeded the
        configured timeout period. It retrieves the session status, and if the session state is IDLE or BUSY and the
        duration since the session start time exceeds the timeout period, the session is terminated. The session ID is
        used to terminate the session via the Athena client.

        Returns:
            None

        """
        session_status = self.get_session_status(session_id)
        if session_status["State"] in ["IDLE", "BUSY"] and (
            session_status["StartDateTime"] - datetime.now(tz=timezone.utc) > timedelta(seconds=self.timeout)
        ):
            LOGGER.debug(f"Terminating session: {session_id}")
            self.athena_client.terminate_session(SessionId=session_id)
        with self.lock:
            LOGGER.debug(f"Releasing lock for session: {session_id}")
            spark_session_locks[UUID(session_id)].release()

    def get_session_status(self, session_id: str) -> Any:
        """
        Get the session status.

        Returns:
            Any: The status of the session
        """
        return self.athena_client.get_session_status(SessionId=session_id)["Status"]

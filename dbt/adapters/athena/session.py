import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from uuid import UUID

import boto3
import boto3.session

from dbt.adapters.athena.constants import LOGGER
from dbt.contracts.connection import Connection
from dbt.exceptions import DbtRuntimeError

DEFAULT_SESSION_COUNT = 4
spark_session_locks: Dict[UUID, threading.Lock] = {}


def get_boto3_session(connection: Connection) -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=connection.credentials.aws_access_key_id,
        aws_secret_access_key=connection.credentials.aws_secret_access_key,
        region_name=connection.credentials.region_name,
        profile_name=connection.credentials.aws_profile_name,
    )


class AthenaSparkSessionManager:
    """
    A helper class to manage Athena Spark Sessions.
    """

    def __init__(self, credentials, timeout: int, polling_interval: float, engine_config: Dict[str, int]):
        self.credentials = credentials
        self.timeout = timeout
        self.polling_interval = polling_interval
        self.engine_config = engine_config
        self.lock = threading.Lock()
        self.athena_client = self.get_athena_client()

    def get_athena_client(self):
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

    def get_new_sessions(self) -> List[UUID]:
        """
        Retrieves a list of new sessions by subtracting the existing sessions from the complete session list.

        Returns:
            List[UUID]: A list of new session UUIDs.

        """
        sessions = self.list_sessions()
        existing_sessions = set(spark_session_locks.keys())
        new_sessions = [session for session in sessions if session not in existing_sessions]
        LOGGER.debug(f"Setting sessions: {new_sessions}")
        return new_sessions

    def update_spark_session_locks(self) -> None:
        """
        Update session locks for each session.

        This function iterates over the existing sessions and ensures that a session lock is created for each session.
        If a session lock already exists, it is left unchanged. After updating the session locks,
        a debug log is generated to display the updated session locks.

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
        Get the session ID.

        This function retrieves the session ID from the stored session information. If session information
        is not available, a new session is started and its session ID is returned.

        Returns:
            str: The session ID.

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

    def list_sessions(self, max_results: int = DEFAULT_SESSION_COUNT, state: str = "IDLE") -> List[UUID]:
        """
        List athena spark sessions.

        This function sends a request to the Athena service to list the sessions in the specified Spark workgroup.
        It filters the sessions by state, only returning the first session that is in IDLE state. If no idle sessions
        are found or if an error occurs, None is returned.

        Returns:
            List[UUID]: A list of session UUIDs if an idle sessions are found, else start a new session and return
            the list of the returned session id.

        """
        response = self.athena_client.list_sessions(
            WorkGroup=self.credentials.spark_work_group, MaxResults=max_results, StateFilter=state
        )
        if len(response.get("Sessions")) == 0 or response.get("Sessions") is None:
            if len(spark_session_locks) < DEFAULT_SESSION_COUNT:
                return [self.start_session()]
            LOGGER.warning(
                f"""Maximum spark session count: {DEFAULT_SESSION_COUNT} reached.
            Cannot start new spark session."""
            )
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

    def poll_until_session_creation(self, session_id) -> None:
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

    def get_session_status(self, session_id) -> Dict[str, Any]:
        """
        Get the session status.

        Returns:
            str: The status of the session
        """
        return self.athena_client.get_session_status(SessionId=session_id)["Status"]

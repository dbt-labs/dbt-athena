import hashlib
import json
import re
import time
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ContextManager, Dict, List, Optional, Tuple

import tenacity
from pyathena.connection import Connection as AthenaConnection
from pyathena.cursor import Cursor
from pyathena.error import OperationalError, ProgrammingError

# noinspection PyProtectedMember
from pyathena.formatter import (
    _DEFAULT_FORMATTERS,
    Formatter,
    _escape_hive,
    _escape_presto,
)
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from pyathena.util import RetryConfig
from tenacity.retry import retry_if_exception
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from dbt.adapters.athena.config import get_boto3_config
from dbt.adapters.athena.session import get_boto3_session
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.events import AdapterLogger
from dbt.exceptions import ConnectionError, DbtRuntimeError

logger = AdapterLogger("Athena")


@dataclass
class AthenaAdapterResponse(AdapterResponse):
    data_scanned_in_bytes: Optional[int] = None


@dataclass
class AthenaCredentials(Credentials):
    s3_staging_dir: str
    region_name: str
    endpoint_url: Optional[str] = None
    work_group: Optional[str] = None
    aws_profile_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    poll_interval: float = 1.0
    debug_query_state: bool = False
    _ALIASES = {"catalog": "database"}
    num_retries: int = 5
    num_boto3_retries: Optional[int] = None
    s3_data_dir: Optional[str] = None
    s3_data_naming: Optional[str] = "schema_table_unique"
    s3_tmp_table_dir: Optional[str] = None
    # Unfortunately we can not just use dict, must be Dict because we'll get the following error:
    # Credentials in profile "athena", target "athena" invalid: Unable to create schema for 'dict'
    seed_s3_upload_args: Optional[Dict[str, Any]] = None
    lf_tags_database: Optional[Dict[str, str]] = None

    @property
    def type(self) -> str:
        return "athena"

    @property
    def unique_field(self) -> str:
        return f"athena-{hashlib.md5(self.s3_staging_dir.encode()).hexdigest()}"

    @property
    def effective_num_retries(self) -> int:
        return self.num_boto3_retries or self.num_retries

    def _connection_keys(self) -> Tuple[str, ...]:
        return (
            "s3_staging_dir",
            "work_group",
            "region_name",
            "database",
            "schema",
            "poll_interval",
            "aws_profile_name",
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "endpoint_url",
            "s3_data_dir",
            "s3_data_naming",
            "s3_tmp_table_dir",
            "debug_query_state",
            "seed_s3_upload_args",
            "lf_tags_database",
        )


class AthenaCursor(Cursor):
    def __init__(self, **kwargs) -> None:  # type: ignore
        super().__init__(**kwargs)
        self._executor = ThreadPoolExecutor()

    def _collect_result_set(self, query_id: str) -> AthenaResultSet:
        query_execution = self._poll(query_id)
        return self._result_set_class(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
        )

    def _poll(self, query_id: str) -> AthenaQueryExecution:
        try:
            query_execution = self.__poll(query_id)
        except KeyboardInterrupt as e:
            if self._kill_on_interrupt:
                logger.warning("Query canceled by user.")
                self._cancel(query_id)
                query_execution = self.__poll(query_id)
            else:
                raise e
        return query_execution

    def __poll(self, query_id: str) -> AthenaQueryExecution:
        while True:
            query_execution = self._get_query_execution(query_id)
            if query_execution.state in [
                AthenaQueryExecution.STATE_SUCCEEDED,
                AthenaQueryExecution.STATE_FAILED,
                AthenaQueryExecution.STATE_CANCELLED,
            ]:
                return query_execution

            if self.connection.cursor_kwargs.get("debug_query_state", False):
                logger.debug(f"Query state is: {query_execution.state}. Sleeping for {self._poll_interval}...")
            time.sleep(self._poll_interval)

    def execute(  # type: ignore
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        catch_partitions_limit: bool = False,
        **kwargs,
    ):
        def inner() -> AthenaCursor:
            query_id = self._execute(
                operation,
                parameters=parameters,
                work_group=work_group,
                s3_staging_dir=s3_staging_dir,
                cache_size=cache_size,
                cache_expiration_time=cache_expiration_time,
            )
            query_execution = self._executor.submit(self._collect_result_set, query_id).result()
            if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
                self.result_set = self._result_set_class(
                    self._connection,
                    self._converter,
                    query_execution,
                    self.arraysize,
                    self._retry_config,
                )

            else:
                raise OperationalError(query_execution.state_change_reason)
            return self

        retry = tenacity.Retrying(
            # No need to retry if TOO_MANY_OPEN_PARTITIONS occurs.
            # Otherwise, Athena throws ICEBERG_FILESYSTEM_ERROR after retry,
            # because not all files are removed immediately after first try to create table
            retry=retry_if_exception(
                lambda e: False if catch_partitions_limit and "TOO_MANY_OPEN_PARTITIONS" in str(e) else True
            ),
            stop=stop_after_attempt(self._retry_config.attempt),
            wait=wait_exponential(
                multiplier=self._retry_config.attempt,
                max=self._retry_config.max_delay,
                exp_base=self._retry_config.exponential_base,
            ),
            reraise=True,
        )
        return retry(inner)


class AthenaConnectionManager(SQLConnectionManager):
    TYPE = "athena"

    @classmethod
    def data_type_code_to_name(cls, type_code: str) -> str:
        """
        Get the string representation of the data type from the Athena metadata. Dbt performs a
        query to retrieve the types of the columns in the SQL query. Then these types are compared
        to the types in the contract config, simplified because they need to match what is returned
        by Athena metadata (we are only interested in the broader type, without subtypes nor granularity).
        """
        return type_code.split("(")[0].split("<")[0].upper()

    @contextmanager  # type: ignore
    def exception_handler(self, sql: str) -> ContextManager:  # type: ignore
        try:
            yield
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            creds: AthenaCredentials = connection.credentials

            handle = AthenaConnection(
                s3_staging_dir=creds.s3_staging_dir,
                endpoint_url=creds.endpoint_url,
                catalog_name=creds.database,
                schema_name=creds.schema,
                work_group=creds.work_group,
                cursor_class=AthenaCursor,
                cursor_kwargs={"debug_query_state": creds.debug_query_state},
                formatter=AthenaParameterFormatter(),
                poll_interval=creds.poll_interval,
                session=get_boto3_session(connection),
                retry_config=RetryConfig(
                    attempt=creds.num_retries + 1,
                    exceptions=("ThrottlingException", "TooManyRequestsException", "InternalServerException"),
                ),
                config=get_boto3_config(num_retries=creds.effective_num_retries),
            )

            connection.state = ConnectionState.OPEN
            connection.handle = handle

        except Exception as exc:
            logger.exception(f"Got an error when attempting to open a Athena connection due to {exc}")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise ConnectionError(str(exc))

        return connection

    @classmethod
    def get_response(cls, cursor: AthenaCursor) -> AthenaAdapterResponse:
        code = "OK" if cursor.state == AthenaQueryExecution.STATE_SUCCEEDED else "ERROR"
        rowcount, data_scanned_in_bytes = cls.process_query_stats(cursor)
        return AthenaAdapterResponse(
            _message=f"{code} {rowcount}",
            rows_affected=rowcount,
            code=code,
            data_scanned_in_bytes=data_scanned_in_bytes,
        )

    @staticmethod
    def process_query_stats(cursor: AthenaCursor) -> Tuple[int, int]:
        """
        Helper function to parse query statistics from SELECT statements.
        The function looks for all statements that contains rowcount or data_scanned_in_bytes,
        then strip the SELECT statements, and pick the value between curly brackets.
        """
        if all(map(cursor.query.__contains__, ["rowcount", "data_scanned_in_bytes"])):
            try:
                query_split = cursor.query.lower().split("select")[-1]
                # query statistics are in the format {"rowcount":1, "data_scanned_in_bytes": 3}
                # the following statement extract the content between { and }
                query_stats = re.search("{(.*)}", query_split)
                if query_stats:
                    stats = json.loads("{" + query_stats.group(1) + "}")
                    return stats.get("rowcount", -1), stats.get("data_scanned_in_bytes", 0)
            except Exception as err:
                logger.debug(f"There was an error parsing query stats {err}")
                return -1, 0
        return cursor.rowcount, cursor.data_scanned_in_bytes

    def cancel(self, connection: Connection) -> None:
        pass

    def add_begin_query(self) -> None:
        pass

    def add_commit_query(self) -> None:
        pass

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass


class AthenaParameterFormatter(Formatter):
    def __init__(self) -> None:
        super().__init__(mappings=deepcopy(_DEFAULT_FORMATTERS), default=None)

    def format(self, operation: str, parameters: Optional[List[str]] = None) -> str:
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        if operation.upper().startswith(("SELECT", "WITH", "INSERT")):
            escaper = _escape_presto
        elif operation.upper().startswith(("VACUUM", "OPTIMIZE")):
            operation = operation.replace('"', "")
        else:
            # Fixes ParseException that comes with newer version of PyAthena
            operation = operation.replace("\n\n    ", "\n")

            escaper = _escape_hive

        kwargs: Optional[List[str]] = None
        if parameters is not None:
            kwargs = list()
            if isinstance(parameters, list):
                for v in parameters:
                    # TODO Review this annoying Decimal hack, unsure if issue in dbt, agate or pyathena
                    if isinstance(v, Decimal) and v == int(v):
                        v = int(v)

                    func = self.get(v)
                    if not func:
                        raise TypeError(f"{type(v)} is not defined formatter.")
                    kwargs.append(func(self, escaper, v))
            else:
                raise ProgrammingError(f"Unsupported parameter (Support for list only): {parameters}")
        return (operation % tuple(kwargs)).strip() if kwargs is not None else operation.strip()

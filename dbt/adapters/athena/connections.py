from typing import ContextManager, Tuple, Optional, List
from dataclasses import dataclass
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal

from pyathena.connection import Connection as AthenaConnection
from pyathena.model import AthenaQueryExecution
from pyathena.error import ProgrammingError
from pyathena.formatter import Formatter
# noinspection PyProtectedMember
from pyathena.formatter import _DEFAULT_FORMATTERS, _escape_hive, _escape_presto

from dbt.adapters.base import Credentials
from dbt.contracts.connection import Connection, AdapterResponse
from dbt.adapters.sql import SQLConnectionManager
from dbt.exceptions import RuntimeException, FailedToConnectException
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class AthenaCredentials(Credentials):
    s3_staging_dir: str
    region_name: str
    schema: str
    work_group: Optional[str]
    _ALIASES = {
        "catalog": "database"
    }
    # TODO Add pyathena.util.RetryConfig ?

    @property
    def type(self) -> str:
        return "athena"

    def _connection_keys(self) -> Tuple[str, ...]:
        return "s3_staging_dir", "work_group", "region_name", "database", "schema"


class AthenaConnectionManager(SQLConnectionManager):
    TYPE = "athena"

    @contextmanager
    def exception_handler(self, sql: str) -> ContextManager:
        try:
            yield
        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            raise RuntimeException(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            creds: AthenaCredentials = connection.credentials

            handle = AthenaConnection(
                s3_staging_dir=creds.s3_staging_dir,
                region_name=creds.region_name,
                schema_name=creds.schema,
                work_group=creds.work_group,
                formatter=AthenaParameterFormatter()
            )

            connection.state = "open"
            connection.handle = handle

        except Exception as e:
            logger.debug("Got an error when attempting to open a Athena "
                         "connection: '{}'"
                         .format(e))
            connection.handle = None
            connection.state = "fail"

            raise FailedToConnectException(str(e))

        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        if cursor.state == AthenaQueryExecution.STATE_SUCCEEDED:
            code = "OK"
        else:
            code = "ERROR"

        return AdapterResponse(
            _message="{} {}".format(code, cursor.rowcount),
            rows_affected=cursor.rowcount,
            code=code
        )

    def cancel(self, connection: Connection):
        connection.handle.cancel()

    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    def begin(self):
        pass

    def commit(self):
        pass


class AthenaParameterFormatter(Formatter):
    def __init__(self) -> None:
        super(AthenaParameterFormatter, self).__init__(
            mappings=deepcopy(_DEFAULT_FORMATTERS), default=None
        )

    def format(
        self, operation: str, parameters: Optional[List[str]] = None
    ) -> str:
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        if operation.upper().startswith("SELECT") or operation.upper().startswith(
            "WITH"
        ):
            escaper = _escape_presto
        else:
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
                        raise TypeError("{0} is not defined formatter.".format(type(v)))
                    kwargs.append(func(self, escaper, v))
            else:
                raise ProgrammingError(
                    "Unsupported parameter "
                    + "(Support for list only): {0}".format(parameters)
                )
        return (operation % tuple(kwargs)).strip() if kwargs is not None else operation.strip()

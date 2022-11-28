import dbt
from dbt.adapters.athena.connections import AthenaConnectionManager, AthenaCredentials
from dbt.adapters.athena.impl import AthenaAdapter
from dbt.adapters.athena.query_headers import _QueryComment
from dbt.adapters.base import AdapterPlugin
from dbt.include import athena

Plugin = AdapterPlugin(adapter=AthenaAdapter, credentials=AthenaCredentials, include_path=athena.PACKAGE_PATH)

# overwrite _QueryComment to add leading "--" to query comment
dbt.adapters.base.query_headers._QueryComment = _QueryComment

__all__ = [
    "AthenaConnectionManager",
    "AthenaCredentials",
    "AthenaAdapter",
    "Plugin",
]

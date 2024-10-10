from dbt.adapters.athena.connections import AthenaConnectionManager, AthenaCredentials
from dbt.adapters.athena.impl import AthenaAdapter
from dbt.adapters.base import AdapterPlugin
from dbt.include import athena

Plugin: AdapterPlugin = AdapterPlugin(
    adapter=AthenaAdapter, credentials=AthenaCredentials, include_path=athena.PACKAGE_PATH
)

__all__ = [
    "AthenaConnectionManager",
    "AthenaCredentials",
    "AthenaAdapter",
    "Plugin",
]

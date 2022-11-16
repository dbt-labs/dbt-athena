from dbt.adapters.base import AdapterPlugin

from dbt.adapters.athena.connections import (  # NOQA
    AthenaConnectionManager,
    AthenaCredentials,
)
from dbt.adapters.athena.impl import AthenaAdapter
from dbt.include import athena

Plugin = AdapterPlugin(adapter=AthenaAdapter, credentials=AthenaCredentials, include_path=athena.PACKAGE_PATH)

from dbt.adapters.athena.column import AthenaColumn
from dbt.adapters.athena.connections import AthenaConnectionManager
from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.impl import AthenaAdapter
import dbt.adapters.athena.query_headers

from dbt.adapters.base import AdapterPlugin
from dbt.include import athena


Plugin = AdapterPlugin(
    adapter=AthenaAdapter,
    credentials=AthenaCredentials,
    include_path=athena.PACKAGE_PATH
)
